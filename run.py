"""
run.py — Operações pós-infraestrutura: populate e simulator.
Lê as saídas do Terraform para obter a URL da API e o nome do bucket S3.
"""

import subprocess
import argparse
import asyncio
import boto3
import csv
import time
import random
import statistics
import osmnx as ox
import httpx
import psycopg2
import pandas as pd
from faker import Faker
from faker.providers import BaseProvider
import redis

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────────────────────────────────────────
REGION       = "us-east-1"
PLACE_NAME   = "São Paulo, Brazil"
NETWORK_TYPE = "drive"

N_CLIENTES     = 5000   # proporção 1:3 com entregadores
N_RESTAURANTES = 1000
N_ENTREGADORES = 3 * N_CLIENTES  # 3x clientes — requisito do professor
CONCURRENCY    = 200
VELOCIDADE_KMH = 2000
fator = 18 / (VELOCIDADE_KMH * 0.1)

P95_SLA = 0.5  # segundos — critério do professor

GLOBAL_API_URL = ""

VOLUMES = {
    "AQUECIMENTO":     5,
    "OPERACAO_NORMAL": 10,
    "PICO":            50,
    "EVENTO_ESPECIAL": 200,
}

RITMO_EXEC = [
    # 120s cobre: boot Fargate (~90s) + ALB health-check + pool de conexões
    # Não entra nos resultados — representa o sistema "já em operação" antes do pico
    {"volume": "AQUECIMENTO",     "duracao": 120, "warmup": True},
    # Linha de base medida
    {"volume": "OPERACAO_NORMAL", "duracao":  120},
    # 120s: auto-scaling reage nos primeiros ~60s e ajuda nos últimos ~60s
    {"volume": "PICO",            "duracao": 120},
    # Burst curto e intenso
    {"volume": "EVENTO_ESPECIAL", "duracao":  60},
]

# Locks e estados compartilhados do simulador
orders                    = {}
orders_lock               = asyncio.Lock()
entregadores_desocupados  = {}
entregadores_desocupados_lock = asyncio.Lock()
entregadores_ocupados     = {}
entregadores_ocupados_lock = asyncio.Lock()
clientes_esperando        = set()
clientes_lock             = asyncio.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# TERRAFORM HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def get_terraform_output(key):
    result = subprocess.run(
        ["terraform", "output", "-raw", key],
        capture_output=True, text=True, cwd="terraform"
    )
    if result.returncode != 0:
        raise RuntimeError(f"Erro ao ler terraform output '{key}':\n{result.stderr}")
    return result.stdout.strip()


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────────────────────────────
def read_tfvars_password():
    with open("terraform/terraform.tfvars", encoding="utf-8") as f:
        for line in f:
            if line.strip().startswith("db_password"):
                return line.split("=")[1].strip().strip('"')
    raise RuntimeError("db_password não encontrado em terraform/terraform.tfvars")


def create_schema():
    host     = get_terraform_output("rds_endpoint")
    password = read_tfvars_password()

    print(f"\n[SCHEMA] Conectando ao RDS: {host}")
    conn = None
    for attempt in range(1, 7):
        try:
            conn = psycopg2.connect(
                host=host, port=5432, dbname="dijkfooddb",
                user="dijk_admin", password=password, connect_timeout=10
            )
            break
        except psycopg2.OperationalError as e:
            print(f"[SCHEMA] Tentativa {attempt}/6 falhou, aguardando 10s... ({e})")
            if attempt == 6:
                raise
            time.sleep(10)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clientes (
                id_cliente SERIAL PRIMARY KEY,
                nome VARCHAR(80), email VARCHAR(80),
                telefone VARCHAR(20), latitude DOUBLE PRECISION, longitude DOUBLE PRECISION
            );
            CREATE TABLE IF NOT EXISTS restaurantes (
                id_restaurante SERIAL PRIMARY KEY,
                nome VARCHAR(80), tipo_cozinha VARCHAR(40),
                latitude DOUBLE PRECISION, longitude DOUBLE PRECISION
            );
            CREATE TABLE IF NOT EXISTS entregadores (
                id_entregador SERIAL PRIMARY KEY,
                nome VARCHAR(80), tipo_veiculo VARCHAR(30),
                status VARCHAR(30) DEFAULT 'AVAILABLE',
                latitude DOUBLE PRECISION, longitude DOUBLE PRECISION
            );
            CREATE TABLE IF NOT EXISTS pedidos (
                id_pedido SERIAL PRIMARY KEY,
                id_cliente INT REFERENCES clientes(id_cliente),
                id_restaurante INT REFERENCES restaurantes(id_restaurante),
                id_entregador INT REFERENCES entregadores(id_entregador),
                lista_itens TEXT, status VARCHAR(30) DEFAULT 'CONFIRMED',
                data DATE, horario TIME
            );
        """)
    conn.commit()
    conn.close()
    print("[SCHEMA] Tabelas criadas com sucesso!\n")


# ─────────────────────────────────────────────────────────────────────────────
# POPULATE
# ─────────────────────────────────────────────────────────────────────────────
def populate(bucket_name):
    print(f"\n[POPULATE] Gerando grafo de '{PLACE_NAME}'...")
    G = ox.graph_from_place(PLACE_NAME, network_type=NETWORK_TYPE)

    with open("graph_nodes.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["node_id", "lat", "lon"])
        for node_id, data in G.nodes(data=True):
            writer.writerow([node_id, data["y"], data["x"]])

    with open("graph_edges.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["from_node", "to_node", "weight"])
        for u, v, data in G.edges(data=True):
            writer.writerow([u, v, data.get("length", 1.0)])

    s3 = boto3.Session(region_name=REGION).client("s3")
    s3.upload_file("graph_nodes.csv", bucket_name, "graph_nodes.csv")
    s3.upload_file("graph_edges.csv", bucket_name, "graph_edges.csv")
    print(f"[POPULATE] Grafo salvo no S3: {bucket_name}\n")


# ─────────────────────────────────────────────────────────────────────────────
# GERADOR DE DADOS FALSOS
# ─────────────────────────────────────────────────────────────────────────────
class RestauranteProvider(BaseProvider):
    tipos_culinaria = [
        'Pizzaria', 'Churrascaria', 'Comida Japonesa', 'Comida Mineira',
        'Comida Baiana', 'Lanchonete', 'Hamburgueria', 'Cantina Italiana',
        'Comida Vegana', 'Bistrô Francês', 'Frutos do Mar'
    ]
    prefixos = ['Restaurante', 'Cantina', 'Pizzaria', 'Bar e Petiscaria', 'Recanto', 'Espaço']
    sufixos  = ['Gourmet', 'da Família', 'Tradicional', 'Express', 'Saboroso', 'Grill']

    def tipo_restaurante(self):
        return self.random_element(self.tipos_culinaria)

    def nome_restaurante(self):
        formato = random.choice(['nome_pessoa', 'prefixo_sufixo', 'sobrenome'])
        if formato == 'nome_pessoa':
            artigo = self.random_element(['do', 'da'])
            nome = self.generator.first_name()
            estabelecimento = self.random_element(['Bar', 'Restaurante', 'Cantina', 'Lanchonete'])
            return f"{estabelecimento} {artigo} {nome}"
        elif formato == 'prefixo_sufixo':
            return f"{self.random_element(self.prefixos)} {self.random_element(self.sufixos)}"
        else:
            estabelecimento = self.random_element(self.prefixos)
            sobrenome = self.generator.last_name()
            return f"{estabelecimento} {sobrenome}"


def get_lat_lon(nodes):
    coords = random.choice(nodes.values)[1:]
    return (float(coords[0]), float(coords[1]))


def gerar_dados_falsos(numero_de_clientes, numero_de_restaurantes, numero_de_entregadores):
    fake = Faker(['pt-BR'])
    fake.add_provider(RestauranteProvider)
    nodes = pd.read_csv("graph_nodes.csv")

    clientes = [
        {
            "nome": fake.name(), "email": fake.email(), "telefone": fake.phone_number(),
            **dict(zip(["latitude", "longitude"], get_lat_lon(nodes)))
        } for _ in range(numero_de_clientes)
    ]
    restaurantes = [
        {
            "nome": fake.nome_restaurante(), "tipo_cozinha": fake.tipo_restaurante(),
            **dict(zip(["latitude", "longitude"], get_lat_lon(nodes)))
        } for _ in range(numero_de_restaurantes)
    ]
    entregadores = [
        {
            "nome": fake.name(),
            "tipo_veiculo": random.choice(['moto', 'carro', 'caminhao', 'biscicleta', 'pé', 'cavalo', 'triciclo', 'chihuahua', 'galinha']),
            **dict(zip(["latitude", "longitude"], get_lat_lon(nodes)))
        } for _ in range(numero_de_entregadores)
    ]
    return (clientes, restaurantes, entregadores)


# ─────────────────────────────────────────────────────────────────────────────
# SIMULADOR — WORKERS ASSÍNCRONOS
# ─────────────────────────────────────────────────────────────────────────────
async def preload(jsons, url):
    """Envia bulk insert e retorna os IDs reais criados no banco."""
    print(f"Enviando lote de {len(jsons)} registros para {url}bulk ...")
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url + "bulk", json=jsons)
        if response.status_code != 200:
            print(f"Erro fatal no Bulk Insert: {response.text}")
            raise Exception(f"Falha ao popular o banco de dados!, {url}bulk")
        n = response.json().get("inseridos", len(jsons))
        print(f"Sucesso! {n} inseridos.")

        # Busca os IDs reais no banco (os últimos N inseridos)
        list_resp = await client.get(url + f"?limit={n}&offset=0")
        if list_resp.status_code == 200:
            items = list_resp.json()
            # Detecta o campo de ID pelo primeiro item da lista
            if items:
                id_field = next((k for k in items[0] if k.startswith("id_")), None)
                if id_field:
                    return [item[id_field] for item in items]
        return list(range(1, n + 1))  # fallback


async def requester(queue, results):
    def alloc_courier(entregador_dict, id_pedido, rota_restaurante, rota_cliente):
        entregador_dict["id_pedido"] = id_pedido
        entregador_dict["edge_idx"] = 0
        entregador_dict["rota_restaurante"] = rota_restaurante
        entregador_dict["rota_cliente"] = rota_cliente
        entregador_dict["rota_atual"] = "rota_restaurante"
        entregador_dict["deslocamento_atual"] = [*entregador_dict["posicao"], 0]
        return entregador_dict

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            start = time.perf_counter()
            response = None
            try:
                if item["method"] == "GET":
                    response = await client.get(item["url"])
                    if response.status_code == 200:
                        dados = response.json()
                        id_pedido      = dados.get("id_pedido", None)
                        id_entregador  = dados.get("id_entregador", None)
                        rota_restaurante = dados.get("rota_restaurante", None)
                        rota_cliente   = dados.get("rota_cliente", None)

                        if item["user"] == "courier" and id_pedido and id_entregador:
                            async with entregadores_desocupados_lock:
                                if id_entregador in entregadores_desocupados:
                                    entregador_dict = entregadores_desocupados[id_entregador]
                                    del entregadores_desocupados[id_entregador]
                                    async with entregadores_ocupados_lock:
                                        entregadores_ocupados[id_entregador] = alloc_courier(
                                            entregador_dict, id_pedido, rota_restaurante, rota_cliente
                                        )

                elif item["method"] in ("POST", "PATCH"):
                    if item["method"] == "POST":
                        response = await client.post(item["url"], json=item["json"])
                        if item["user"] == "client" and item["url"].endswith("/pedidos/"):
                            if response.status_code == 200:
                                pedido_criado = response.json()
                                async with orders_lock:
                                    orders[pedido_criado["id_pedido"]] = {
                                        "status": "CONFIRMED",
                                        "id_cliente": item["json"]["id_cliente"]
                                    }
                            else:
                                async with clientes_lock:
                                    clientes_esperando.discard(item["json"]["id_cliente"])
                    else:
                        response = await client.patch(item["url"], json=item["json"])

                    if item["user"] == "courier" and item["json"].get("novo_status", "") == "PICKED_UP":
                        await queue.put({
                            "method": "PATCH",
                            "url": item["url"],
                            "json": {"novo_status": "IN_TRANSIT", "id_entregador": item["json"].get("id_entregador")},
                            "ritmo_idx": item["ritmo_idx"],
                            "user": "courier"
                        })

                latency = time.perf_counter() - start
                results.append({
                    "status": response.status_code if response is not None else "desconhecido",
                    "latency": latency,
                    "ritmo_idx": item["ritmo_idx"],
                    "method": item["method"],
                    "user": item["user"]
                })

            except Exception as e:
                if item["method"] == "POST" and item["user"] == "client" and item["url"].endswith("/pedidos/"):
                    async with clientes_lock:
                        clientes_esperando.discard(item["json"]["id_cliente"])
                latency = time.perf_counter() - start
                results.append({
                    "status": "error", "latency": latency, "error": str(e),
                    "ritmo_idx": item["ritmo_idx"], "method": item["method"], "user": item["user"]
                })

            queue.task_done()


async def restaurant_updater(queue, current_ritmo, duracao):
    start = time.perf_counter()
    volume_cozinha = 20

    while time.perf_counter() - start < duracao:
        current_start = time.perf_counter()
        delta = random.expovariate(volume_cozinha)
        ritmo_idx = current_ritmo[0]

        try:
            async with orders_lock:
                current_orders = dict(orders)

            preparando  = [id_p for id_p, p in current_orders.items() if p["status"] == "PREPARING"]
            confirmados = [id_p for id_p, p in current_orders.items() if p["status"] == "CONFIRMED"]

            id_pedido = None
            novo_status = None

            if preparando and (random.random() <= 0.8 or not confirmados):
                id_pedido = random.choice(preparando)
                novo_status = "READY_FOR_PICKUP"
            elif confirmados:
                id_pedido = random.choice(confirmados)
                novo_status = "PREPARING"

            if id_pedido and novo_status:
                await queue.put({
                    "method": "PATCH",
                    "url": f"{GLOBAL_API_URL}/pedidos/{id_pedido}/status",
                    "json": {"novo_status": novo_status},
                    "ritmo_idx": ritmo_idx,
                    "user": "restaurant"
                })
                async with orders_lock:
                    orders[id_pedido]["status"] = novo_status

        except Exception:
            await asyncio.sleep(0.01)
            continue

        delta_sleep = current_start + delta - time.perf_counter()
        await asyncio.sleep(max(delta_sleep, 0))


async def move_courier(id_entregador):
    fim_rota   = False
    mudanca_rota = False

    def compute_new_desl(entregador):
        edge_idx  = entregador["edge_idx"]
        rota_atual = entregador["rota_atual"]
        if not entregador[rota_atual]:
            return 0
        if edge_idx >= len(entregador[rota_atual]):
            return 1
        coord_atual, coord_nova, metros = entregador[rota_atual][edge_idx]
        lat,  long  = float(coord_atual[0]), float(coord_atual[1])
        new_lat, new_long = float(coord_nova[0]), float(coord_nova[1])
        metros = float(metros)
        dist_100ms = max(1, int((fator / 10) * metros + 0.5))
        entregador["deslocamento_atual"] = [(new_lat - lat) / dist_100ms, (new_long - long) / dist_100ms, dist_100ms]
        entregador["posicao"] = lat, long
        entregador["edge_idx"] += 1
        return 0

    async with entregadores_ocupados_lock:
        couriers = dict(entregadores_ocupados)

    entregador = couriers.get(id_entregador, None)
    if not entregador:
        return None, None, None

    if entregador["deslocamento_atual"][2] <= 0:
        fim_rota = compute_new_desl(entregador)

    lat, long = entregador["posicao"]
    lat_desl, long_desl, dist_100ms = entregador["deslocamento_atual"]

    if dist_100ms > 0:
        entregador["posicao"] = lat + lat_desl, long + long_desl
        entregador["deslocamento_atual"][2] = dist_100ms - 1
        mudanca_rota = False
    elif fim_rota:
        rota_atual = entregador["rota_atual"]
        if rota_atual == "rota_restaurante":
            id_pedido = int(entregador["id_pedido"])
            async with orders_lock:
                if id_pedido not in orders:
                    return lat, long, None
                status = orders[id_pedido]["status"]
            if status != "READY_FOR_PICKUP":
                return lat, long, None
            entregador["rota_atual"] = "rota_cliente"
            entregador["edge_idx"] = 0
            async with orders_lock:
                orders[id_pedido]["status"] = "IN_TRANSIT"
            mudanca_rota = True
        elif rota_atual == "rota_cliente":
            return None, None, None

    async with entregadores_ocupados_lock:
        entregadores_ocupados[id_entregador] = entregador

    return entregador["posicao"][0], entregador["posicao"][1], mudanca_rota


async def updater_courier(queue, current_ritmo, duracao):
    def desalloc_courier(entregador_dict):
        for key in ["id_pedido", "edge_idx", "rota_restaurante", "rota_cliente", "rota_atual", "deslocamento_atual"]:
            entregador_dict[key] = None
        return entregador_dict

    start = time.perf_counter()
    current_100ms = 0.0

    while time.perf_counter() - start < duracao:
        try:
            ritmo_idx = current_ritmo[0]
            async with entregadores_ocupados_lock:
                couriers = dict(entregadores_ocupados)

            if couriers:
                for id_entregador in list(couriers.keys()):
                    lat, long, mudanca_rota = await move_courier(id_entregador)
                    id_pedido = int(couriers[id_entregador]["id_pedido"])

                    if not lat or not long:
                        await queue.put({"method": "POST", "url": f"{GLOBAL_API_URL}/alocacoes/{id_entregador}/desativar/{id_pedido}", "json": {}, "ritmo_idx": ritmo_idx, "user": "courier"})
                        await queue.put({"method": "PATCH", "url": f"{GLOBAL_API_URL}/pedidos/{id_pedido}/status", "json": {"novo_status": "DELIVERED", "id_entregador": id_entregador}, "ritmo_idx": ritmo_idx, "user": "courier"})

                        async with orders_lock:
                            if id_pedido in orders:
                                id_cli = orders[id_pedido].get("id_cliente")
                                del orders[id_pedido]
                                if id_cli:
                                    async with clientes_lock:
                                        clientes_esperando.discard(id_cli)

                        async with entregadores_ocupados_lock:
                            entregador_dict = entregadores_ocupados[id_entregador]
                            del entregadores_ocupados[id_entregador]
                        async with entregadores_desocupados_lock:
                            entregadores_desocupados[id_entregador] = desalloc_courier(entregador_dict)
                    else:
                        await queue.put({"method": "POST", "url": f"{GLOBAL_API_URL}/entregadores/{id_entregador}/posicao", "json": {"latitude": lat, "longitude": long}, "ritmo_idx": ritmo_idx, "user": "courier"})
                        if mudanca_rota:
                            await queue.put({"method": "PATCH", "url": f"{GLOBAL_API_URL}/pedidos/{id_pedido}/status", "json": {"novo_status": "PICKED_UP", "id_entregador": id_entregador}, "ritmo_idx": ritmo_idx, "user": "courier"})

        except Exception as e:
            print(f"\n[ERRO CRÍTICO NO SIMULADOR] O entregador travou: {e}")

        current_100ms += 0.1
        delta = start + current_100ms - time.perf_counter()
        await asyncio.sleep(max(delta, 0))


async def viewer_courier(queue, volume, duracao, ritmo_idx):
    start = time.perf_counter()
    current_second = 0
    volume *= 3
    times = sorted([t for t in [random.expovariate(volume) for _ in range(volume)] if t < 1])

    while time.perf_counter() - start < duracao:
        for each_time in times:
            async with entregadores_desocupados_lock:
                couriers = list(entregadores_desocupados.keys())
            if not couriers:
                await asyncio.sleep(0.01)
                continue
            id_entregador = random.choice(couriers)
            await asyncio.sleep(max(start + current_second + each_time - time.perf_counter(), 0))
            await queue.put({
                "method": "GET",
                "url": f"{GLOBAL_API_URL}/alocacoes/{id_entregador}/acompanhamento",
                "params": {}, "ritmo_idx": ritmo_idx, "user": "courier"
            })
        current_second += 1
        await asyncio.sleep(max(start + current_second - time.perf_counter(), 0))


async def producer_order(queue, volume, duracao, ritmo_idx, clientes, restaurantes):
    start = time.perf_counter()
    while time.perf_counter() - start < duracao:
        current_start = time.perf_counter()
        delta = random.expovariate(volume)
        async with clientes_lock:
            livres = list(set(clientes) - clientes_esperando)
        if not livres:
            await asyncio.sleep(0.01)
            continue
        id_cli = random.choice(livres)
        async with clientes_lock:
            clientes_esperando.add(id_cli)
        await queue.put({
            "method": "POST",
            "url": f"{GLOBAL_API_URL}/pedidos/",
            "json": {"id_cliente": id_cli, "id_restaurante": random.choice(restaurantes), "lista_itens": []},
            "ritmo_idx": ritmo_idx, "user": "client"
        })
        await asyncio.sleep(max(current_start + delta - time.perf_counter(), 0))


async def viewer_order(queue, volume, duracao, ritmo_idx):
    start = time.perf_counter()
    await asyncio.sleep(max(start + 1 - time.perf_counter(), 0))
    while time.perf_counter() - start < duracao + 1:
        current_start = time.perf_counter()
        delta = random.expovariate(volume)
        try:
            async with orders_lock:
                order_id = random.choice(list(orders.keys()))
        except Exception:
            await asyncio.sleep(0.01)
            continue
        await queue.put({
            "method": "GET",
            "url": f"{GLOBAL_API_URL}/pedidos/{order_id}/acompanhamento",
            "params": {}, "ritmo_idx": ritmo_idx, "user": "client"
        })
        await asyncio.sleep(max(current_start + delta - time.perf_counter(), 0))


# ─────────────────────────────────────────────────────────────────────────────
# SIMULADOR — ORQUESTRADOR
# ─────────────────────────────────────────────────────────────────────────────
async def run_simulation():
    print(f"\n[SIMULADOR] Iniciando tráfego contra a API: {GLOBAL_API_URL}")

    data = gerar_dados_falsos(N_CLIENTES, N_RESTAURANTES, N_ENTREGADORES)

    ids_clientes, ids_restaurantes, ids_entregadores = await asyncio.gather(
        preload(data[0], GLOBAL_API_URL + "/clientes/"),
        preload(data[1], GLOBAL_API_URL + "/restaurantes/"),
        preload(data[2], GLOBAL_API_URL + "/entregadores/"),
    )

    clientes     = ids_clientes
    restaurantes = ids_restaurantes

    async with entregadores_desocupados_lock:
        for id_e, courier in zip(ids_entregadores, data[2]):
            entregadores_desocupados[id_e] = {
                "id_pedido": None, "posicao": [courier["latitude"], courier["longitude"]],
                "edge_idx": None, "rota_restaurante": None, "rota_cliente": None,
                "rota_atual": None, "deslocamento_atual": None
            }

    queue   = asyncio.Queue()
    results = []

    requesters = [asyncio.create_task(requester(queue, results)) for _ in range(CONCURRENCY)]

    total_duracao = sum(r["duracao"] for r in RITMO_EXEC)
    current_ritmo = [0]

    motores = [
        asyncio.create_task(updater_courier(queue, current_ritmo, total_duracao)),
        asyncio.create_task(restaurant_updater(queue, current_ritmo, total_duracao)),
    ]

    producers = None
    for idx, ritmo in enumerate(RITMO_EXEC):
        volume  = VOLUMES[ritmo["volume"]]
        duracao = ritmo["duracao"]
        label   = "[AQUECIMENTO]" if ritmo.get("warmup") else f"RITMO {ritmo['volume']}"
        current_ritmo[0] = idx
        print(f"{label} por {duracao} segundos")
        producers = [
            asyncio.create_task(producer_order(queue, volume, duracao, idx, clientes, restaurantes)),
            asyncio.create_task(viewer_order(queue, volume, duracao, idx)),
            asyncio.create_task(viewer_courier(queue, volume, duracao, idx)),
        ]
        await asyncio.gather(*producers)

    if producers:
        await asyncio.gather(*producers)
    await asyncio.gather(*motores)

    if not queue.empty():
        print(f"\n[Aguardando a AWS] Drenando {queue.qsize()} requisições restantes...")
        while not queue.empty():
            print(f"Faltam processar: {queue.qsize()} requisições...    ", end="\r")
            await asyncio.sleep(0.5)

    await queue.join()
    print("\nTodas as requisições foram processadas e respondidas pela API da AWS!")

    for _ in requesters:
        await queue.put(None)
    await asyncio.gather(*requesters)

    def percentil(data, p):
        data_sorted = sorted(data)
        k = int(len(data_sorted) * p / 100)
        return data_sorted[min(k, len(data_sorted) - 1)]

    # Exclui o warm-up dos resultados medidos
    fases_medidas = [i for i, r in enumerate(RITMO_EXEC) if not r.get("warmup")]

    latencias = {i: {"PATCH": [], "POST": [], "GET": []} for i in range(len(RITMO_EXEC))}
    erros      = {i: 0 for i in range(len(RITMO_EXEC))}
    for r in results:
        latencias[r["ritmo_idx"]][r["method"]].append(r["latency"])
        if r["status"] not in (200, 201, 400, 404):
            erros[r["ritmo_idx"]] += 1

    metrics = {
        "Mean":   statistics.mean,
        "Median": statistics.median,
        "P95":    lambda x: percentil(x, 95),
        "Max":    max,
    }

    # Larguras fixas para alinhamento perfeito
    W_VOL    = 24   # coluna Volume
    W_METHOD =  7   # coluna método (PATCH/POST/GET)
    W_VAL    = 10   # cada coluna de valor
    W_SLA    =  8   # coluna SLA P95

    def fmt_val(fn, data):
        return f"{round(fn(data), 4):>{W_VAL}}" if data else f"{'':>{W_VAL}}"

    sep = (
        "+" + "-" * (W_VOL + W_METHOD + 3) +
        ("+" + "-" * (W_VAL + 2)) * len(metrics) +
        "+" + "-" * (W_SLA + 2) + "+"
    )

    total_medido = sum(len(latencias[i][m]) for i in fases_medidas for m in latencias[i])
    print(f"\n===== RESULTADOS DA SIMULAÇÃO (SLA: P95 < {P95_SLA}s) =====")
    print(f"Total de requisições medidas: {total_medido}  |  warm-up excluído\n")

    header = (
        f"| {'Volume':<{W_VOL}} {'':>{W_METHOD}} |" +
        "".join(f" {m:^{W_VAL}} |" for m in metrics) +
        f" {'SLA P95':^{W_SLA}} |"
    )
    print(sep)
    print(header)
    print(sep)

    sla_ok_geral = True
    for idx in fases_medidas:
        nome      = RITMO_EXEC[idx]["volume"]
        dura      = RITMO_EXEC[idx]["duracao"]
        taxa_erro = erros[idx] / max(sum(len(v) for v in latencias[idx].values()), 1) * 100
        subtitulo = f"{dura}s | erros:{taxa_erro:.1f}%"
        prefixos  = [f"{nome}", subtitulo, ""]

        for prefix, method, latencia in zip(prefixos, latencias[idx].keys(), latencias[idx].values()):
            if latencia:
                p95_val      = percentil(latencia, 95)
                sla_ok       = p95_val < P95_SLA
                sla_ok_geral = sla_ok_geral and sla_ok
                sla_str      = "  OK  " if sla_ok else " FAIL "
            else:
                sla_str = "  --  "

            row = (
                f"| {prefix:<{W_VOL}} {method:>{W_METHOD}} |" +
                "".join(f" {fmt_val(fn, latencia)} |" for fn in metrics.values()) +
                f" {sla_str:^{W_SLA}} |"
            )
            print(row)
        print(sep)

    resultado = "[PASSOU]" if sla_ok_geral else "[FALHOU]"
    detalhe   = f"Todos os P95 < {P95_SLA}s" if sla_ok_geral else f"Algum P95 >= {P95_SLA}s"
    print(f"\n{resultado} {detalhe}\n")




# ─────────────────────────────────────────────────────────────────────────────
# CLEAN (HARD RESET)
# ─────────────────────────────────────────────────────────────────────────────
def clean_databases():
    print("\n[CLEAN] ⚠️ ATENÇÃO: Isso vai deletar TUDO do Redis, RDS e DynamoDB.")
    confirmacao = input("Digite 'DELETAR' para confirmar: ")
    
    if confirmacao != "DELETAR":
        print("[CLEAN] Operação abortada. Seus dados estão a salvo.\n")
        return

    # 1. Limpeza do RDS (PostgreSQL)
    try:
        print("\n[CLEAN] Iniciando limpeza do RDS...")
        host = get_terraform_output("rds_endpoint")
        password = read_tfvars_password()
        
        conn = psycopg2.connect(
            host=host, port=5432, dbname="dijkfooddb",
            user="dijk_admin", password=password, connect_timeout=10
        )
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE pedidos, clientes, restaurantes, entregadores CASCADE;")
        conn.commit()
        conn.close()
        print("  ✅ Todas as tabelas do RDS esvaziadas (CASCADE executado).")
    except Exception as e:
        print(f"  ❌ Erro ao limpar RDS: {e}")

    # 2. Limpeza do Redis
    try:
        print("\n[CLEAN] Iniciando limpeza do REDIS...")
        try:
            redis_host = get_terraform_output("redis_endpoint")
        except RuntimeError:
            redis_host = "localhost" # Fallback caso o túnel/proxy local esteja rodando
            
        r = redis.Redis(host=redis_host, port=6379, decode_responses=True)
        r.flushall()
        print("  ✅ Todas as chaves do Redis apagadas com sucesso.")
    except Exception as e:
        print(f"  ❌ Erro ao limpar Redis: {e}")

    # 3. Limpeza do DynamoDB
    try:
        print("\n[CLEAN] Iniciando limpeza do DYNAMODB...")
        dynamodb = boto3.resource("dynamodb", region_name=REGION)
        tabelas_dynamo = [
            "dijkfood-historico-eventos",
            "dijkfood-telemetria-entregadores",
            "dijkfood-alocacao-entregadores"
        ]

        for nome_tabela in tabelas_dynamo:
            print(f"  -> Verificando {nome_tabela}...")
            try:
                tabela = dynamodb.Table(nome_tabela)
                chaves_esquema = [k['AttributeName'] for k in tabela.key_schema]
                
                # Paginação no Scan
                response = tabela.scan()
                itens = response.get('Items', [])
                
                while 'LastEvaluatedKey' in response:
                    response = tabela.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                    itens.extend(response.get('Items', []))

                if not itens:
                    print(f"    [OK] Tabela {nome_tabela} já estava vazia.")
                    continue

                # Exclusão em lote
                apagados = 0
                with tabela.batch_writer() as batch:
                    for item in itens:
                        chave_delecao = {k: item[k] for k in chaves_esquema}
                        batch.delete_item(Key=chave_delecao)
                        apagados += 1

                print(f"    ✅ {apagados} itens deletados de {nome_tabela}.")
            except Exception as e:
                print(f"    ❌ Erro na tabela {nome_tabela}: {e}")
                
        print("\n🚀 Hard reset concluído com sucesso!\n")
    except Exception as e:
        print(f"  ❌ Erro no fluxo do DynamoDB: {e}")



# ─────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────
def main():
    global GLOBAL_API_URL
    parser = argparse.ArgumentParser(description="Operações pós-deploy DijkFood")
    parser.add_argument("--step", choices=["schema", "populate", "simulator", "clean"], required=True,
                        help="schema: cria as tabelas no RDS | populate: envia o grafo ao S3 | simulator: dispara tráfego na API | clean: esvazia bancos")
    args = parser.parse_args()

    if args.step == "schema":
        create_schema()

    elif args.step == "populate":
        bucket_name = get_terraform_output("s3_bucket")
        populate(bucket_name)

    elif args.step == "simulator":
        GLOBAL_API_URL = get_terraform_output("api_url")
        print(f"[SIMULADOR] API URL: {GLOBAL_API_URL}")
        asyncio.run(run_simulation())
        
    elif args.step == "clean":
        clean_databases()

if __name__ == "__main__":
    main()
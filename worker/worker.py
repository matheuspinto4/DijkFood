import os
import time
import boto3
import psycopg2
import csv
import heapq
import math
import json 
from collections import defaultdict
from botocore.config import Config
from datetime import datetime
from decimal import Decimal

# ---------------------------------------------------------------------------
# Configurações de Ambiente & Nuvem
# ---------------------------------------------------------------------------
DYNAMODB_REGION = os.getenv("AWS_REGION", "us-east-1")
boto_config = Config(max_pool_connections=50)

# SQS e DynamoDB
sqs_client = boto3.client("sqs", region_name=DYNAMODB_REGION, config=boto_config)
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

dynamodb = boto3.resource("dynamodb", region_name=DYNAMODB_REGION, config=boto_config)
NOME_TABELA_ALOCACOES = os.getenv("DDB_ALOCACOES", "dijkfood-alocacao-entregadores")
tabela_alocacoes = dynamodb.Table(NOME_TABELA_ALOCACOES)

# RDS & S3
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")

API_URL = os.getenv("API_URL", "http://localhost:8000") 

# ---------------------------------------------------------------------------
# Funções Matemáticas e Algoritmos
# ---------------------------------------------------------------------------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

def encontrar_no_mais_proximo(lat, lon, dict_nodes):
    no_mais_proximo = None
    menor_distancia = float('inf')
    for node_id, coords in dict_nodes.items():
        dist = haversine(lat, lon, coords['lat'], coords['lon'])
        if dist < menor_distancia:
            menor_distancia = dist
            no_mais_proximo = node_id
    return no_mais_proximo

def dijkstra(graph: dict[int, list[tuple[int, float]]], source: int):
    dist = {source: 0.0}
    prev = {source: None}
    heap = [(0.0, source)]

    while heap:
        d_u, u = heapq.heappop(heap)
        if d_u > dist.get(u, float("inf")):
            continue
        for v, w in graph.get(u, []):
            alt = d_u + w
            if alt < dist.get(v, float("inf")):
                dist[v] = alt
                prev[v] = u
                heapq.heappush(heap, (alt, v))
    return dist, prev

def extrair_caminho(prev: dict[int, int], source: int, target: int) -> list[int]:
    caminho = []
    atual = target
    if atual not in prev and atual != source:
        return [] 
    while atual is not None:
        caminho.insert(0, atual)
        atual = prev.get(atual)
    return caminho

def formatar_rota_dynamo(caminho: list[int], graph: dict, dict_nodes: dict) -> list:
    rota_formatada = []
    for i in range(len(caminho) - 1):
        u = caminho[i]
        v = caminho[i+1]
        
        coord_u = (Decimal(str(dict_nodes[u]['lat'])), Decimal(str(dict_nodes[u]['lon'])))
        coord_v = (Decimal(str(dict_nodes[v]['lat'])), Decimal(str(dict_nodes[v]['lon'])))
        
        peso_aresta = 0.0
        for vizinho, peso in graph.get(u, []):
            if vizinho == v:
                peso_aresta = peso
                break
                
        rota_formatada.append([coord_u, coord_v, Decimal(str(peso_aresta))])
    return rota_formatada

# ---------------------------------------------------------------------------
# Funções de Nuvem e Carregamento 
# ---------------------------------------------------------------------------
def load_graph(path: str) -> dict[int, list[tuple[int, float]]]:
    graph: dict[int, list[tuple[int, float]]] = defaultdict(list)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            u, v, w = int(row["from_node"]), int(row["to_node"]), float(row["weight"])
            graph[u].append((v, w))
    return graph

def load_nodes(path: str) -> dict:
    nodes = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            nodes[int(row["node_id"])] = {"lat": float(row["lat"]), "lon": float(row["lon"])}
    return nodes

def download_graph_from_s3():
    print(f"[S3] Baixando grafos do Bucket '{S3_BUCKET}'...")
    s3 = boto3.client('s3', region_name="us-east-1")
    s3.download_file(S3_BUCKET, "graph_edges.csv", "/tmp/graph_edges.csv")
    s3.download_file(S3_BUCKET, "graph_nodes.csv", "/tmp/graph_nodes.csv")
    print("[S3] Download concluído com sucesso!")

# ---------------------------------------------------------------------------
# Worker Principal
# ---------------------------------------------------------------------------
def main():
    print("[WORKER] Iniciando o serviço de cálculo de rotas com SQS...")
    
    if S3_BUCKET:
        download_graph_from_s3()
        edges_path, nodes_path = "/tmp/graph_edges.csv", "/tmp/graph_nodes.csv"
    else:
        edges_path, nodes_path = "graph_edges.csv", "graph_nodes.csv"
        
    graph = load_graph(edges_path)
    dict_nodes = load_nodes(nodes_path)
    print(f"[WORKER] Grafo carregado com {len(dict_nodes)} nós na memória.")

    conn = None
    while not conn:
        try:
            conn = psycopg2.connect(host=DB_HOST, port=5432, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
            conn.autocommit = False
            print("[WORKER] Conectado ao PostgreSQL (RDS)!")
        except Exception as e:
            print("[WORKER] Aguardando banco de dados...")
            time.sleep(5)

    while True:
        try:
            # 1. RECEBER MENSAGEM DA FILA SQS (Long Polling de 10s)
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )

            # Se a fila estiver vazia, recomeça o loop
            if 'Messages' not in response:
                continue

            # Pega a mensagem e o handle (identificador para poder deletar depois)
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']
            
            # Converte a String JSON de volta para Dicionário
            pedido_dados = json.loads(message['Body'])
            
            id_pedido = pedido_dados['id_pedido']
            print(f"\n[WORKER] Novo Pedido SQS Recebido! ID: {id_pedido} ...")

            # 2. EXTRAIR COORDENADAS 
            res_lat = float(pedido_dados['latitude_restaurante'])
            res_lon = float(pedido_dados['longitude_restaurante'])
            cli_lat = float(pedido_dados['latitude_cliente'])
            cli_lon = float(pedido_dados['longitude_cliente'])

            with conn.cursor() as cur:
                # 3. BUSCAR ENTREGADORES
                cur.execute("""
                    SELECT id_entregador, latitude, longitude 
                    FROM entregadores 
                    WHERE status = 'AVAILABLE' AND latitude IS NOT NULL;
                """)
                entregadores_disponiveis = cur.fetchall()

                if not entregadores_disponiveis:
                    print(f"[WORKER] Nenhum entregador disponível no momento.")
                    conn.rollback()
                    # Não deletamos a mensagem! Assim, após alguns segundos, ela reaparece na fila
                    # para tentarmos de novo quando um entregador ficar disponível.
                    time.sleep(2)
                    continue

                # --- Lógica de Matemática de Rotas ---
                candidatos = []
                for id_ent, lat_ent, lon_ent in entregadores_disponiveis:
                    dist_reta = haversine(res_lat, res_lon, lat_ent, lon_ent)
                    candidatos.append((dist_reta, id_ent, lat_ent, lon_ent))
                
                candidatos.sort(key=lambda x: x[0])
                top_3_candidatos = candidatos[:3]

                no_restaurante = encontrar_no_mais_proximo(res_lat, res_lon, dict_nodes)
                no_cliente = encontrar_no_mais_proximo(cli_lat, cli_lon, dict_nodes)
                
                melhor_entregador = None
                menor_distancia_rua = float('inf')
                caminho_ate_restaurante = []

                start_time = time.perf_counter()
                for _, id_ent, lat_ent, lon_ent in top_3_candidatos:
                    no_entregador = encontrar_no_mais_proximo(lat_ent, lon_ent, dict_nodes)
                    distancias, prev = dijkstra(graph, no_entregador)
                    dist_real = distancias.get(no_restaurante, float('inf'))
                    
                    if dist_real < menor_distancia_rua:
                        menor_distancia_rua = dist_real
                        melhor_entregador = id_ent
                        caminho_ate_restaurante = extrair_caminho(prev, no_entregador, no_restaurante)

                elapsed = time.perf_counter() - start_time

                if melhor_entregador is None or not caminho_ate_restaurante:
                    print(f"[WORKER] Caminho impossível pelas ruas.")
                    conn.rollback()
                    time.sleep(2)
                    continue

                distancias_cli, prev_cli = dijkstra(graph, no_restaurante)
                caminho_ate_cliente = extrair_caminho(prev_cli, no_restaurante, no_cliente)

                rota_restaurante = formatar_rota_dynamo(caminho_ate_restaurante, graph, dict_nodes)
                rota_cliente = formatar_rota_dynamo(caminho_ate_cliente, graph, dict_nodes)
                # -----------------------------------------------------

                # 4. RESERVAR O ENTREGADOR NO RDS (Evitando que 2 workers peguem o mesmo)
                cur.execute("""
                    SELECT id_entregador FROM entregadores 
                    WHERE id_entregador = %s AND status = 'AVAILABLE' 
                    FOR UPDATE SKIP LOCKED;
                """, (melhor_entregador,))
                
                if not cur.fetchone():
                    print(f"[WORKER] Entregador {melhor_entregador} já ocupado. Recomeçando...")
                    conn.rollback()
                    continue

                print(f"[WORKER] Entregador {melhor_entregador} escolhido! Cálculo: {elapsed:.4f}s")

                try:
                    # 5. ATUALIZAR BANCOS (Dynamo e RDS)
                    tabela_alocacoes.put_item(
                        Item={
                            "id_entregador": str(melhor_entregador),
                            "timestamp": datetime.utcnow().isoformat(),
                            "status": "ATIVO",
                            "id_pedido": id_pedido,
                            "rota_restaurante": rota_restaurante,
                            "rota_cliente": rota_cliente
                        }
                    )
                    
                    cur.execute("UPDATE entregadores SET status = 'BUSY' WHERE id_entregador = %s;", (melhor_entregador,))
                    cur.execute("UPDATE pedidos SET id_entregador = %s WHERE id_pedido = %s;", (melhor_entregador, id_pedido))
                    conn.commit()
                    
                    # 6. MÁGICA FINAL: TUDO DEU CERTO, APAGA A MENSAGEM DA FILA SQS
                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=receipt_handle
                    )
                    print(f"[WORKER] Pedido {id_pedido} processado e removido do SQS com sucesso!")
                    
                except Exception as e:
                    print(f"[WORKER] Erro gravando nos bancos: {e}")
                    conn.rollback()
                    # Como deu erro, não apagamos a mensagem. Ela voltará para a fila.

        except Exception as e:
            conn.rollback()
            print(f"[ERRO] Falha no processamento: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
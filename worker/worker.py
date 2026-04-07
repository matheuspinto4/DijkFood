import os
import time
import boto3
import psycopg2
import csv
import heapq
import requests
import math
from collections import defaultdict

# ---------------------------------------------------------------------------
# Configurações de Ambiente
# ---------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
S3_BUCKET = os.getenv("S3_BUCKET")
API_URL = os.getenv("API_URL", "http://localhost:8000") # Endpoint da API REST (ALB)

# ---------------------------------------------------------------------------
# Funções Matemáticas e Algoritmos
# ---------------------------------------------------------------------------
def haversine(lat1, lon1, lat2, lon2):
    """Calcula a distância em linha reta entre dois pontos (em km)"""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

def encontrar_no_mais_proximo(lat, lon, dict_nodes):
    """Traduz uma coordenada de GPS para o ID do nó da rua mais próximo"""
    no_mais_proximo = None
    menor_distancia = float('inf')
    for node_id, coords in dict_nodes.items():
        dist = haversine(lat, lon, coords['lat'], coords['lon'])
        if dist < menor_distancia:
            menor_distancia = dist
            no_mais_proximo = node_id
    return no_mais_proximo

def dijkstra(graph: dict[int, list[tuple[int, float]]], source: int) -> dict[int, float]:
    """Calcula os caminhos mais curtos a partir do nó fonte (source)"""
    dist: dict[int, float] = {source: 0.0}
    heap: list[tuple[float, int]] = [(0.0, source)]

    while heap:
        d_u, u = heapq.heappop(heap)
        if d_u > dist.get(u, float("inf")):
            continue
        for v, w in graph.get(u, []):
            alt = d_u + w
            if alt < dist.get(v, float("inf")):
                dist[v] = alt
                heapq.heappush(heap, (alt, v))
    return dist

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
    print("[WORKER] Iniciando o serviço de cálculo de rotas...")
    
    # 1. Traz o mapa para a RAM
    if S3_BUCKET:
        download_graph_from_s3()
        edges_path, nodes_path = "/tmp/graph_edges.csv", "/tmp/graph_nodes.csv"
    else:
        edges_path, nodes_path = "graph_edges.csv", "graph_nodes.csv"
        
    graph = load_graph(edges_path)
    dict_nodes = load_nodes(nodes_path)
    print(f"[WORKER] Grafo carregado com {len(dict_nodes)} nós na memória.")

    # 2. Conecta ao PostgreSQL
    conn = None
    while not conn:
        try:
            conn = psycopg2.connect(host=DB_HOST, port=5432, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
            conn.autocommit = False
            print("[WORKER] Conectado ao PostgreSQL (RDS)!")
        except Exception as e:
            print("[WORKER] Aguardando banco de dados...")
            time.sleep(5)

    # 3. Loop infinito (Consumidor de fila)
    while True:
        try:
            with conn.cursor() as cur:
                # A. Tranca 1 Pedido que esteja aguardando roteamento
                cur.execute("""
                    SELECT id_pedido, id_restaurante FROM pedidos 
                    WHERE status = 'CONFIRMED' 
                    FOR UPDATE SKIP LOCKED LIMIT 1;
                """)
                row_pedido = cur.fetchone()

                if not row_pedido:
                    conn.rollback()
                    time.sleep(1) # Dorme 1 segundo se a fila estiver vazia
                    continue

                id_pedido, id_restaurante = row_pedido
                print(f"\n[WORKER] Processando Pedido ID: {id_pedido} ...")

                # B. Lê onde fica o Restaurante
                cur.execute("SELECT latitude, longitude FROM restaurantes WHERE id_restaurante = %s;", (id_restaurante,))
                res_lat, res_lon = cur.fetchone()

                # C. Lê os Entregadores Disponíveis (sem trancar ainda, para podermos calcular o mais rápido)
                cur.execute("""
                    SELECT id_entregador, latitude, longitude 
                    FROM entregadores 
                    WHERE status = 'AVAILABLE' AND latitude IS NOT NULL;
                """)
                entregadores_disponiveis = cur.fetchall()

                if not entregadores_disponiveis:
                    print(f"[WORKER] Nenhum entregador disponível no momento.")
                    conn.rollback()
                    time.sleep(2)
                    continue

                # D. HEURÍSTICA: Corta caminho filtrando os 3 mais próximos em linha reta
                candidatos = []
                for id_ent, lat_ent, lon_ent in entregadores_disponiveis:
                    dist_reta = haversine(res_lat, res_lon, lat_ent, lon_ent)
                    candidatos.append((dist_reta, id_ent, lat_ent, lon_ent))
                
                candidatos.sort(key=lambda x: x[0])
                top_3_candidatos = candidatos[:3]

                # E. Encontra o ID do cruzamento (nó) do restaurante no mapa
                no_restaurante = encontrar_no_mais_proximo(res_lat, res_lon, dict_nodes)
                
                melhor_entregador = None
                menor_distancia_rua = float('inf')

                # F. Roda o Dijkstra APENAS nos top 3 candidatos
                start_time = time.perf_counter()
                for _, id_ent, lat_ent, lon_ent in top_3_candidatos:
                    no_entregador = encontrar_no_mais_proximo(lat_ent, lon_ent, dict_nodes)
                    distancias = dijkstra(graph, no_entregador)
                    dist_real = distancias.get(no_restaurante, float('inf'))
                    
                    if dist_real < menor_distancia_rua:
                        menor_distancia_rua = dist_real
                        melhor_entregador = id_ent

                elapsed = time.perf_counter() - start_time

                if melhor_entregador is None:
                    print(f"[WORKER] Caminho impossível pelas ruas para todos os candidatos.")
                    conn.rollback()
                    continue

                # G. Tranca o entregador vencedor no banco (evita que outro Worker o roube)
                cur.execute("""
                    SELECT id_entregador FROM entregadores 
                    WHERE id_entregador = %s AND status = 'AVAILABLE' 
                    FOR UPDATE SKIP LOCKED;
                """, (melhor_entregador,))
                
                if not cur.fetchone():
                    # Entregador foi atribuído a outro pedido no último milissegundo!
                    print(f"[WORKER] Entregador {melhor_entregador} já ocupado. Reprocessando o pedido...")
                    conn.rollback()
                    continue

                print(f"[WORKER] Entregador {melhor_entregador} escolhido! Cálculo: {elapsed:.4f}s")

                # H. Atualiza as relações no PostgreSQL
                cur.execute("UPDATE entregadores SET status = 'BUSY' WHERE id_entregador = %s;", (melhor_entregador,))
                cur.execute("UPDATE pedidos SET id_entregador = %s WHERE id_pedido = %s;", (melhor_entregador, id_pedido))

                # I. CHAMA A API REST: Cumpre o requisito e aciona o histórico do DynamoDB
                patch_url = f"{API_URL}/pedidos/{id_pedido}/status"
                response = requests.patch(patch_url, json={"novo_status": "PREPARING"}, timeout=5)
                
                if response.status_code == 200:
                    conn.commit() # Confirma tudo atómicamente!
                    print(f"[WORKER] Sucesso! Pedido {id_pedido} avançou via API. Entregador {melhor_entregador} a caminho.")
                else:
                    print(f"[WORKER] Erro na API REST ({response.status_code}): {response.text}")
                    conn.rollback()
                    time.sleep(1)

        except Exception as e:
            conn.rollback()
            print(f"[ERRO] Falha no processamento: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
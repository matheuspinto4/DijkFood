import json
import base64
import os
import redis
from datetime import datetime

# from shapely.geometry import shape, Point
# from shapely.strtree import STRtree
# from pyproj import Transformer
# from shapely.geometry import Point

# transformer = Transformer.from_crs(
#     "EPSG:4326",   # lat/lon
#     "EPSG:31983",  # GeoSampa
#     always_xy=True
# )

# # Configuracoes de poligonos para obter regioes
# with open("geoportal_distrito_municipal_v2.geojson", encoding="utf-8") as f:
#     geojson = json.load(f)

# poligonos = []
# distritos = []
# regioes = []

# for feature in geojson["features"]:
#     poligonos.append(shape(feature["geometry"]))
#     distritos.append(feature["properties"]["nm_distrito_municipal"])
#     regioes.append(feature["properties"]["nm_regiao_05"])
    
# tree = STRtree(poligonos)


# Ordem de mudanca de status
PREVIOUS_STATE = {
    "PREPARING": "CONFIRMED",
    "READY_FOR_PICKUP": "PREPARING",
    "PICKED_UP": "READY_FOR_PICKUP",
    "IN_TRANSIT": "PICKED_UP",
    "DELIVERED": "IN_TRANSIT",
}
STATES = ["CONFIRMED", "PREPARING", "READY_FOR_PICKUP", "PICKED_UP", "IN_TRANSIT", "DELIVERED"]



print(f"[INIT] Conectando ao Redis: {os.environ.get('REDIS_HOST', 'NAO DEFINIDO')}")

r = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ.get("REDIS_PORT", 6379)),
    decode_responses=True
)

print("[INIT] Redis conectado com sucesso")

def handler(event, context):
    records = event.get("Records", [])
    print(f"[HANDLER] Recebidos {len(records)} registros do Kinesis")

    for record in records:
        payload = json.loads(
            base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        )
        stream = record["eventSourceARN"].split("/")[-1]
        print(f"[HANDLER] stream={stream} payload={json.dumps(payload)[:100]}")
        process(stream, payload)

    return {"statusCode": 200}

def obter_distrito_regiao(lat, lon):
    return None, None
    if lat is None or lon is None: return None, None
    x, y = transformer.transform(lon, lat)
    ponto = Point(x, y)

    indices_candidatos = tree.query(ponto)
    print(indices_candidatos)

    for poligono_idx in indices_candidatos:
        if poligonos[poligono_idx].contains(ponto):
            return distritos[poligono_idx], regioes[poligono_idx]

    return None, None


def process(stream, payload):
    print(f"[DEBUG] Stream recebida: {stream} - payload: {payload}")
    try:
        if "new-order" in stream:
            id_pedido = payload.get("id_pedido", None)
            id_restaurante = payload.get("id_restaurante", None)
            timestamp = payload.get("timestamp", datetime.utcnow().isoformat())
            status = payload.get("status", "UNKNOWN")
            lat = payload.get("latitude_cliente", None)
            lon = payload.get("longitude_cliente", None)
            lista_itens = payload.get("lista_itens", [])
            if not id_pedido is None:
                # Atualiza metricas agregadas
                r.sadd("orders:active", id_pedido)
                r.incr("throughput:orders")
                r.expire("throughput:orders", 60)
                r.hincrby("orders:status", status, 1)
                
                # Atualiza dados de cada pedido e item
                distrito, regiao = obter_distrito_regiao(lat, lon)
                r.hset("orders:data", key=id_pedido,
                    value=json.dumps({
                        "restaurante": id_restaurante,
                        "timestamp_start": timestamp,
                        "duracao": None, 
                        "distrito": distrito,
                        "regiao": regiao
                    })
                )
                r.hset("orders:state", id_pedido,
                    value=json.dumps({
                        "status": status,
                        "timestamp": timestamp,
                        "timers": {state:None for state in STATES}
                    })
                )
                for item in lista_itens:
                    r.hincrby("itens:quantity", item, 1)
                
            print(f"[REDIS] orders-new-order → id={id_pedido} ativos={r.scard('orders:active')} - payload={payload}")
        
        elif "order-events" in stream:
            id_pedido = payload.get("id_pedido", None)
            timestamp_str = payload.get("timestamp", datetime.utcnow().isoformat())
            timestamp = datetime.fromisoformat(timestamp_str)
            status = payload.get("status", "UNKNOWN")
            
            # Inicialização segura como Fallback para impedir UnboundLocalError
            timers = {state: None for state in STATES}
            
            # Atualiza as metricas agregadas
            previous_data = r.hget("orders:state", id_pedido)
            if previous_data:
                previous_data = json.loads(previous_data)
                previous_status = previous_data["status"]
                previous_timestamp = datetime.fromisoformat(previous_data["timestamp"])
                timers = previous_data["timers"]
                previous_status_duration = timestamp - previous_timestamp
                timers[previous_status] = previous_status_duration.total_seconds()
                r.hincrby("orders:status", previous_status, -1)
            r.hincrby("orders:status", status, 1)
            
            # Atualiza os dados do pedido
            r.hset("orders:state", id_pedido,
                value=json.dumps({
                    "status": status,
                    "timestamp": timestamp_str, 
                    "timers": timers
                })
            )
            # Finaliza o pedido caso esteja no ultimo estado
            if status == STATES[-1]:
                r.srem("orders:active", id_pedido)
                order_data = r.hget("orders:data", id_pedido)
                if order_data:
                    order_data = json.loads(order_data)
                    timestamp_start = datetime.fromisoformat(order_data["timestamp_start"])
                    duracao = timestamp - timestamp_start
                    order_data["duracao"] = duracao.total_seconds()
                    r.hset("orders:data", id_pedido, value=json.dumps(order_data))
                
            
            print(f"[REDIS] order-events → status={status} total={r.hget('orders:status',status)}")

        elif "courier-positions" in stream:
            cid = payload.get("id_entregador")
            if cid:
                r.hset(f"courier:pos:{cid}", mapping={
                    "lat": payload.get("latitude", 0),
                    "lon": payload.get("longitude", 0)
                })
                r.expire(f"courier:pos:{cid}", 30)
                r.sadd("couriers:active", cid)
                r.expire("couriers:active", 30)
                print(f"[REDIS] courier-positions → id={cid} ativos={r.scard('couriers:active')}")

        elif "allocation-events" in stream:
            r.incr("allocations:total")
            print(f"[REDIS] allocation-events → total={r.get('allocations:total')}")

        else:
            print(f"[HANDLER] stream desconhecido: {stream}")

    except Exception as e:
        print(f"[ERRO] Falha ao processar {stream}: {type(e).__name__}: {e}")
        raise
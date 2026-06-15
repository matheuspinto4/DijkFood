import json
import base64
import os
import redis
from datetime import datetime

from shapely.geometry import shape, Point
from shapely.strtree import STRtree
from pyproj import Transformer
from shapely.geometry import Point

transformer = Transformer.from_crs(
    "EPSG:4326",   # lat/lon
    "EPSG:31983",  # GeoSampa
    always_xy=True
)

# Configuracoes de poligonos para obter regioes
with open("geoportal_distrito_municipal_v2.geojson", encoding="utf-8") as f:
    geojson = json.load(f)

poligonos = []
distritos = []
regioes = []

for feature in geojson["features"]:
    poligonos.append(shape(feature["geometry"]))
    distritos.append(feature["properties"]["nm_distrito_municipal"])
    regioes.append(feature["properties"]["nm_regiao_05"])
    
tree = STRtree(poligonos)


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
            timestamp_str = payload.get("timestamp", datetime.utcnow().isoformat())
            timestamp = datetime.fromisoformat(timestamp_str)
            status = payload.get("status", "UNKNOWN")
            lat = payload.get("latitude_cliente", None)
            lon = payload.get("longitude_cliente", None)
            lista_itens = payload.get("lista_itens", [])
            if not id_pedido is None:
                # Atualiza metricas agregadas
                r.sadd("orders:active", id_pedido)
                r.incr("throughput:orders")
                r.hincrby("orders:status", status, 1)
                distrito, regiao = obter_distrito_regiao(lat, lon)
                r.hincrby("orders:distrito", distrito, 1)
                r.hincrby("orders:regiao", regiao, 1)
                hour = timestamp.strftime("%H")
                day = timestamp.isoweekday()
                r.hincrby("orders:week_demand", f"{day}-{hour}", 1)
                r.zincrby("restaurants:volume", 1, id_restaurante)
                
                # Atualiza dados de cada pedido e item
                r.hset("orders:data", key=id_pedido,
                    value=json.dumps({
                        "restaurante": id_restaurante,
                        "timestamp_start": timestamp_str,
                        "duracao": None, 
                        "distrito": distrito,
                        "regiao": regiao
                    })
                )
                r.hset("orders:state", id_pedido,
                    value=json.dumps({
                        "status": status,
                        "timestamp": timestamp_str,
                        "timers": {state:None for state in STATES}
                    })
                )
                for item in lista_itens:
                    r.hincrby("itens:quantity", item, 1)
                
            print(f"[REDIS] orders-new-order → id={id_pedido} ativos={r.scard('orders:active')}")# - payload={payload}")
        
        elif "order-events" in stream:
            id_pedido = payload.get("id_pedido", None)
            timestamp_str = payload.get("timestamp", datetime.utcnow().isoformat())
            timestamp = datetime.fromisoformat(timestamp_str)
            status = payload.get("status", "UNKNOWN")
            
            # Inicializacao segura como Fallback para impedir UnboundLocalError
            timers = {state: None for state in STATES}
            
            # Atualiza as metricas agregadas
            previous_data = r.hget("orders:state", id_pedido)
            if previous_data:
                previous_data = json.loads(previous_data)
                previous_status = previous_data["status"]
                previous_timestamp = datetime.fromisoformat(previous_data["timestamp"])
                timers = previous_data["timers"]
                duration = timestamp - previous_timestamp
                duration = duration.total_seconds()
                timers[previous_status] = duration
                r.hincrby("orders:status_hist_quantity", previous_status, 1)
                r.hincrbyfloat("orders:status_hist_durations", previous_status, duration)
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
                    duracao = duracao.total_seconds()
                    order_data["duracao"] = duracao
                    r.hset("orders:data", id_pedido, value=json.dumps(order_data))
                    r.hincrby("orders:duration_dist", int(duracao), 1)
                
            
            print(f"[REDIS] order-events → status={status} total={r.hget('orders:status',status)}")

        elif "courier-positions" in stream:
            cid = payload.get("id_entregador")
            if cid:
                r.hset(f"courier:pos:{cid}", mapping={
                    "lat": payload.get("latitude", 0),
                    "lon": payload.get("longitude", 0)
                })
                r.expire(f"courier:pos:{cid}", 30)
                print(f"[REDIS] courier-positions → id={cid}")

        elif "allocation-events" in stream:
            id_entregador = payload.get("id_entregador")
            status = payload.get("status")
            timestamp_str = payload.get("timestamp")
            if timestamp_str is None:
                print(f"[REDIS-ERROR] allocation-events → timestamp missing!")
                return
            timestamp = datetime.fromisoformat(timestamp_str) 
            previous_timestamp_str = r.hget("couriers:allocs", id_entregador)
            if previous_timestamp_str is None:
                print(f"[REDIS-PASS] allocation-events → first-alloc-courier{id_entregador}")
            else:
                previous_timestamp = datetime.fromisoformat(previous_timestamp_str)
                duration = timestamp - previous_timestamp
                duration = duration.total_seconds()
                if status == "INATIVA":
                    r.srem("couriers:active", id_entregador)
                    r.incrbyfloat("couriers:busy_time", duration)
                else:
                    r.sadd("couriers:active", id_entregador)
                    r.incrbyfloat("couriers:idle_time", duration)
                print(f"[REDIS] allocation-events → total={r.scard('couriers:active')}")
            r.hset("couriers:allocs", id_entregador, timestamp_str)

        else:
            print(f"[HANDLER] stream desconhecido: {stream}")

    except Exception as e:
        print(f"[ERRO] Falha ao processar {stream}: {type(e).__name__}: {e}")
        raise
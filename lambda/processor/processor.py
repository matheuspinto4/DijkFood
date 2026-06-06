import json
import base64
import os
import redis
import datetime

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
    try:
        if "new-order" in stream:
            id_pedido = payload.get("id_pedido", None)
            id_restaurante = payload.get("id_restaurante", None)
            timestamp = payload.get("timestamp", datetime.utcnow().isoformat())
            status = payload.get("status", "UNKNOWN")
            lat = payload.get("latitude_cliente", None)
            lon = payload.get("longitude_cliente", None)
            lista_itens = payload.get("lista_itens", [])
            r.incr("orders:quantity")
            if id_pedido:
                r.sadd("orders:active", id_pedido)
                r.sadd("restaurante:active", id_restaurante)
                r.set(f"orders:restaurante:{id_pedido}", value=id_restaurante)
                r.set(f"orders:timestamp:{id_pedido}", value=timestamp)
                r.set(f"orders:status:{id_pedido}", value=status)
                distrito, regiao = obter_distrito_regiao(lat, lon)
                r.set(f"orders:distrito:{id_pedido}", value=distrito)
                r.set(f"orders:region:{id_pedido}", value=regiao)
                for item in lista_itens:
                    r.incr(f"item:quantity:{item}")
                
                print(f"[REDIS] orders-new-order → id={id_pedido} ativos={r.scard('orders:active')}")
        
        elif "order-events" in stream:
            status = payload.get("status", "UNKNOWN")
            r.incr(f"orders:status:{status}")
            r.incr("orders:total")
            r.incr("throughput:orders")
            r.expire("throughput:orders", 60)
            print(f"[REDIS] order-events → status={status} total={r.get('orders:total')}")

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
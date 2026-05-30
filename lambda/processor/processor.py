import json
import base64
import os
import redis

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

def process(stream, payload):
    try:
        if "order-events" in stream:
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
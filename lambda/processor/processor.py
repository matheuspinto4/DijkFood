import json
import base64
import os
import redis

r = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ.get("REDIS_PORT", 6379)),
    decode_responses=True
)

def handler(event, context):
    for record in event["Records"]:
        payload = json.loads(
            base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        )
        stream = record["eventSourceARN"].split("/")[-1]
        process(stream, payload)
    return {"statusCode": 200}

def process(stream, payload):
    if "order-events" in stream:
        status = payload.get("status", "UNKNOWN")
        r.incr(f"orders:status:{status}")
        r.incr("orders:total")
        r.incr("throughput:orders")
        r.expire("throughput:orders", 60)

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

    elif "allocation-events" in stream:
        r.incr("allocations:total")

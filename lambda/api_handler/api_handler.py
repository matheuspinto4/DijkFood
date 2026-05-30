import json
import os
import redis

r = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ.get("REDIS_PORT", 6379)),
    decode_responses=True
)

def handler(event, context):
    path = event.get("rawPath", "")

    if "/metrics/orders" in path:
        return resp(200, {
            "total": int(r.get("orders:total") or 0),
            "por_status": {
                "CONFIRMED":       int(r.get("orders:status:CONFIRMED") or 0),
                "PREPARING":       int(r.get("orders:status:PREPARING") or 0),
                "READY_FOR_PICKUP":int(r.get("orders:status:READY_FOR_PICKUP") or 0),
                "IN_TRANSIT":      int(r.get("orders:status:IN_TRANSIT") or 0),
                "DELIVERED":       int(r.get("orders:status:DELIVERED") or 0),
            }
        })

    elif "/metrics/entregadores" in path:
        return resp(200, {
            "ativos": r.scard("couriers:active")
        })

    elif "/metrics/throughput" in path:
        return resp(200, {
            "pedidos_ultimo_minuto": int(r.get("throughput:orders") or 0),
            "alocacoes_total":       int(r.get("allocations:total") or 0),
        })

    return resp(404, {"erro": "rota não encontrada"})

def resp(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }

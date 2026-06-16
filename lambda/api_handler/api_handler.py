import json
import os
import redis

STATES = ["CONFIRMED", "PREPARING", "READY_FOR_PICKUP", "PICKED_UP", "IN_TRANSIT", "DELIVERED"]

r = redis.Redis(
    host=os.environ["REDIS_HOST"],
    port=int(os.environ.get("REDIS_PORT", 6379)),
    decode_responses=True
)

def handler(event, context):
    path = event.get("rawPath") or event.get("path", "")

    if "/metrics/orders" in path:
        orders_status = r.hgetall("orders:status")
        orders_status = {
            status: int(orders_status.get(status) or 0)
            for status in STATES
        }
        orders_regioes = {
            regiao: int(quantity)
            for regiao, quantity in r.hgetall("orders:regiao").items()
        }
        orders_weekdayhour = {
            weekdayhour: int(quantity)
            for weekdayhour, quantity in r.hgetall("orders:week_demand").items()
        }
        orders_status_qtt = r.hgetall("orders:status_hist_quantity")
        orders_status_dur = r.hgetall("orders:status_hist_durations")
        orders_status_duration = {
            status: float(orders_status_dur.get(status) or 0) / max(int(orders_status_qtt.get(status) or 0), 1)
            for status in STATES
        }
        top_restaurants = [
            {
                "id": rid,
                "pedidos": int(volume)
            }
            for rid, volume in r.zrevrange(
                "restaurants:volume",
                0,
                9,
                withscores=True
            )
        ]
        return resp(200, {
            "por_status": orders_status,
            "por_regiao": orders_regioes, 
            "por_dia_semana": orders_weekdayhour, 
            "top_10_restaurants": top_restaurants, 
            "duracao_media_por_status": orders_status_duration,
            "histograma_duracao": {
                int(k): int(v)
                for k, v in r.hgetall("orders:duration_dist").items()
            },
            "itens": {
                k: int(v)
                for k, v in r.hgetall("itens:quantity").items()
            },
            "quantidade": r.scard("orders:active"),
            "total": r.get("throughput:orders"),
        })

    elif "/metrics/entregadores" in path:
        return resp(200, {
            "ativos": r.scard("couriers:active"),
            "busy_time": float(r.get("couriers:busy_time") or 0), 
            "idle_time": float(r.get("couriers:idle_time") or 0)
        })

    elif "/metrics/throughput" in path:
        return resp(200, {
            "pedidos_ultimo_minuto": int(r.get("throughput:orders") or 0),
            "alocacoes_total":       int(r.get("allocations:total") or 0),
        })
        
    elif "/metrics/restaurantes" in path:
        return resp(200, {
            "volumes_maximos": {
                int(k): int(v)
                for k, v in r.hgetall("restaurants:volume").items()
            },
        })

    return resp(404, {"erro": "rota não encontrada"})

def resp(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body)
    }
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
        # Pega apenas os IDs dos pedidos em andamento
        ativos = list(r.smembers("orders:active"))
        orders = {}
        if ativos:
            # Faz o fetch em lote (hmget) apenas dos pedidos ativos
            raw_data = r.hmget("orders:data", ativos)
            raw_state = r.hmget("orders:state", ativos)

            for id_pedido, d_str, s_str in zip(ativos, raw_data, raw_state):
                data = json.loads(d_str) if d_str else {}
                state = json.loads(s_str) if s_str else {}
                
                timers = state.pop("timers", {})
                orders[id_pedido] = {**data, **state, **timers}

        return resp(200, {
            "por_status": {
                k: int(v)
                for k, v in r.hgetall("orders:status").items()
            },
            "itens": {
                k: int(v)
                for k, v in r.hgetall("itens:quantity").items()
            },
            "quantidade": r.scard("orders:active"),
            "orders": orders
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
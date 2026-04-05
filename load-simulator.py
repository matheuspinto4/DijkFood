import asyncio
import httpx
import time
import statistics
import random


# Define variaveis
N_CLIENTES     = 1000
N_RESTAURANTES = 50
N_ENTREGADORES = 3 * N_CLIENTES
N_DDB_ITEMS    = 50000

URL = "http://localhost:8000"
CONCURRENCY = 40 # Delay de 0.5s da api + max volume 200, essa capacidade da conta de criar aos pedidos no segundo em que foram planejados.
VOLUMES = {
    "OPERACAO_NORMAL": 10,
    "PICO": 50,
    "EVENTO_ESPECIAL": 200
}
RITMO_EXEC = [
    {
        "volume": "OPERACAO_NORMAL",
        "duracao": 60
    },
    {
        "volume": "PICO",
        "duracao": 20
    },
    {
        "volume": "EVENTO_ESPECIAL",
        "DURACAO": 40
    },
    {
        "volume": "OPERACAO_NORMAL",
        "DURACAO": 20
    },
    {
        "volume": "PICO",
        "DURACAO": 40
    }
]


# Popular o sistema com dados basicos


# Cliente pode consultar o status do pedido (estado, entregador e posicao)
# API deve responder em menos de 500ms no 95 percentil
orders = []
orders_lock = asyncio.Lock()

async def main():
    queue = asyncio.Queue()
    results = []

    # Cria requesters para realizarem as requisicoes a api
    requesters = [
        asyncio.create_task(requester(queue, results))
        for _ in range(CONCURRENCY)
    ]

    for ritmo in RITMO_EXEC:
        producers = [
            asyncio.create_task(producer_order(queue, ritmo["volume"], ritmo["duracao"])),
            asyncio.create_task(view_order(queue, ritmo["duracao"])),
        ]

        start_ritmo = time.perf_counter()

        await asyncio.gather(*producers)

        await queue.join()

        ritmo_time = time.perf_counter() - start_ritmo

    # Finaliza requesters
    for _ in requesters:
        await queue.put(None)

    await asyncio.gather(*requesters)
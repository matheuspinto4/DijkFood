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

    for idx in range(len(RITMO_EXEC)):
        ritmo = RITMO_EXEC[idx]
        volume = VOLUMES[ritmo["volume"]]
        duracao = ritmo["duracao"]
        producers = [
            asyncio.create_task(producer_order(queue, volume, duracao, idx)),
            asyncio.create_task(view_order(queue, duracao, idx)),
        ]

        start_ritmo = time.perf_counter()
        await asyncio.gather(*producers)
        await queue.join()
        ritmo_time = time.perf_counter() - start_ritmo
        RITMO_EXEC[idx]["real_time"] = ritmo_time
        

    # Finaliza requesters
    for _ in requesters:
        await queue.put(None)

    await asyncio.gather(*requesters)
    
    
    def percentil(data, p):
        data_sorted = sorted(data)
        k = int(len(data_sorted) * p / 100)
        return data_sorted[min(k, len(data_sorted) - 1)]

    latencias = {i: [] for i in range(RITMO_EXEC)}
    for r in results:
        ritmo_idx = r["ritmo_idx"]
        latency = r["latency"]
        latencias[ritmo_idx].append(latency)


    print("\n===== RESULTADOS =====")
    print(f"Total de requisições: {len(results)}")

    print("\nLatência:")
    ritmos_names = [r["volume"] for r in RITMO_EXEC]
    print(f"|{'Metrica':^10}|", *[f"{ritmo:^20}|"                                 for ritmo in ritmos_names])
    print(f"|{'Min':^10}|",     *[f"{round(min(latencia), 4):^20}|"               for latencia in latencias.values()])
    print(f"|{'Media':^10}|",   *[f"{round(statistics.mean(latencia), 4):^20}|"   for latencia in latencias.values()])
    print(f"|{'Mediana':^10}|", *[f"{round(statistics.median(latencia), 4):^20}|" for latencia in latencias.values()])
    print(f"|{'p95':^10}|",     *[f"{round(percentil(latencia, 95), 4):^20}|"     for latencia in latencias.values()])
    print(f"|{'Máx':^10}|",     *[f"{round(max(latencia), 4):^20}|"               for latencia in latencias.values()])

    ritmos_times = [r["real_time"] for r in RITMO_EXEC]
    print(f"|{'Real time':^10}|", *[f"{total_time:^20}|" for total_time in ritmos_times])
    
    
if __name__ == "__main__":
    asyncio.run(main())
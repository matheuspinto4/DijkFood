import asyncio
import httpx
import time
import statistics
import random

from fake_data import gerar_dados_falsos


# Define variaveis
N_CLIENTES     = 1000
N_RESTAURANTES = 50
N_ENTREGADORES = 3 * N_CLIENTES
N_DDB_ITEMS    = 50000

URL = "http://localhost:8000"
CONCURRENCY = 40
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
        "duracao": 40
    },
    {
        "volume": "OPERACAO_NORMAL",
        "duracao": 20
    },
    {
        "volume": "PICO",
        "duracao": 40
    }
]


# Popular o sistema com dados basicos
async def worker(semaphore, client, url, json, results, id_register):
    async with semaphore:
        response = await client.post(
            url,
            json=json
        )
        results.append(response[id_register]) 
        return response.status_code

async def preload(jsons, url, id_register, ids):
    semaphore = asyncio.Semaphore(CONCURRENCY)
    results = []

    async with httpx.AsyncClient() as client:
        tasks = [
            worker(semaphore, client, url, json, results, id_register)
            for json in jsons
        ]
        await asyncio.gather(*tasks)
    ids[id_register] = results

# Cliente pode consultar o status do pedido (estado, entregador e posicao)
# API deve responder em menos de 500ms no 95 percentil
async def requester(queue, results):
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            ritmo_idx = item["ritmo_idx"]
            method = item["method"]
            try:
                start = time.perf_counter()
                if method == "GET":
                    response = await client.get(
                        item["url"],
                        # params=item.get("params")
                    )
                elif method == "POST":
                    response = await client.post(
                        item["url"],
                        json=item.get("json")
                    )
                latency = time.perf_counter() - start

                results.append({
                    "status": response.status_code,
                    "latency": latency,
                    "ritmo_idx": ritmo_idx,
                    "method": method
                })

            except Exception as e:
                latency = time.perf_counter() - start

                results.append({
                    "status": "error",
                    "latency": latency,
                    "error": str(e),
                    "ritmo_idx": ritmo_idx,
                    "method": method
                })

            queue.task_done()
            

async def producer_order(queue, volume, duration, ritmo_idx, clientes, restaurantes):
    start = time.perf_counter()
    orders_acum = 0

    while time.perf_counter() - start < duration:
        # Produz pedidos seguindo uma distribuicao de poisson
        delta = random.expovariate(volume)
        await asyncio.sleep(delta)

        async with orders_lock:
            order_id = orders_acum
            orders.append(order_id)
            orders_acum += 1

        # Escolhe aleatoriamente um cliente e um restaurante
        id_cli = random.choice(clientes)
        id_res = random.choice(restaurantes)
        json = {"id_cliente": id_cli, "id_restaurante": id_res, "lista_itens": []}
        
        await queue.put({
            "method": "POST",
            "url": f"{URL}/pedidos",
            "json": json, 
            "ritmo_idx": ritmo_idx
        })


async def viewer_order(queue, duration, ritmo_idx):
    start = time.perf_counter()
    await asyncio.sleep(start + 1 - time.perf_counter())
    current_sec = 1
    
    # Comeca no final do primeiro segundo e termina no final do ultimo (depois da criacao do ultimo pedido)
    while time.perf_counter() - start < duration + 1:
        async with orders_lock:
            current_orders = list(orders)
        
        if not current_orders:
            await asyncio.sleep(0.01)
            continue
        
        # Consulta todos os pedidos disponiveis no segundo atual de forma uniforme
        times = sorted(start + current_sec + random.uniform(0, 1) for _ in current_orders)
        for order_id, time_exec in zip(current_orders, times):
            # Pula para o tempo sorteado para a execucao
            delta = time_exec - time.perf_counter()
            await asyncio.sleep(max(0, delta))
            
            await queue.put({
                "method": "GET",
                "url": f"{URL}/pedidos/{order_id}/acompanhamento",
                "params": {}, 
                "ritmo_idx": ritmo_idx
            })
        current_sec += 1

orders = []
orders_lock = asyncio.Lock()

async def main():
    data = gerar_dados_falsos( # clientes, restaurantes, entregadores
        numero_de_clientes=N_CLIENTES, 
        numero_de_restaurantes=N_RESTAURANTES, 
        numero_de_entregadores=N_ENTREGADORES
    )

    ids = {}
    tasks = [
        preload(jsons, URL + path, id_register, ids)
        for jsons, path, id_register in zip(
            data,
            ["/clientes", "/restaurantes", "/entregadores"],
            ["id_cliente", "id_restaurante", "id_entregador"]
        )
    ]

    await asyncio.gather(*tasks)
    clientes = ids["id_cliente"]
    restaurantes = ids["id_restaurante"]
    
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
            asyncio.create_task(producer_order(queue, volume, duracao, idx, clientes, restaurantes)),
            asyncio.create_task(viewer_order(queue, duracao, idx)),
        ]

        await asyncio.gather(*producers)
        await queue.join()
        

    # Finaliza requesters
    for _ in requesters:
        await queue.put(None)

    await asyncio.gather(*requesters)
    
    def percentil(data, p):
        data_sorted = sorted(data)
        k = int(len(data_sorted) * p / 100)
        return data_sorted[min(k, len(data_sorted) - 1)]

    # Separa as latencias de leitura e escrita
    latencias = {i: {"POST": [], "GET": []} for i in range(len(RITMO_EXEC))}
    for r in results:
        ritmo_idx = r["ritmo_idx"]
        latency = r["latency"]
        method = r["method"]
        latencias[ritmo_idx][method].append(latency)

    # Metricas
    print("\n===== RESULTADOS =====")
    print(f"Total de requisições: {len(results)}")
    print("\nLatência:")
    
    metrics = {
        "Min": min, 
        "Mean": statistics.mean, 
        "Median": statistics.median, 
        "P95": lambda x: percentil(x, 95), 
        "Max": max,
    }
    seccion = "-" * (len(metrics) * 12 + 32)
    print(f"|{'Volume':^30}|", *[f"{metric:^10}|" for metric in metrics.keys()])
    print(seccion)
    for ritmo_idx, ritmo_latencia in latencias.items():
        ritmo_name = RITMO_EXEC[ritmo_idx]["volume"]
        prefixos = [f"{ritmo_idx+1}. {ritmo_name}", ""]
        for prefix, sufix, latencia in zip(prefixos, ritmo_latencia.keys(), ritmo_latencia.values()):
            print(f"| {prefix:<19}", f"{sufix:>9}|", *[f"{round(f(latencia), 4) if latencia != [] else '':^10}|" for f in metrics.values()])
        print(seccion)
    
    
if __name__ == "__main__":
    asyncio.run(main())
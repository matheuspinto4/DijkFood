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
# URL = "http://dijkfood-api-alb-980772795.us-east-1.elb.amazonaws.com"
CONCURRENCY = 40
VOLUMES = {
    "OPERACAO_NORMAL": 10,
    "PICO": 50,
    "EVENTO_ESPECIAL": 200
}
RITMO_EXEC = [
    {
        "volume": "OPERACAO_NORMAL",
        "duracao": 3#10#60
    },
    # {
    #     "volume": "PICO",
    #     "duracao": 20
    # },
    # {
    #     "volume": "EVENTO_ESPECIAL",
    #     "duracao": 40
    # },
    # {
    #     "volume": "OPERACAO_NORMAL",
    #     "duracao": 20
    # },
    # {
    #     "volume": "PICO",
    #     "duracao": 40
    # }
]

# ================================ MUDEI AQUI ============================
# async def worker(semaphore, client, url, json, results, id_register):
#     async with semaphore:
#         response = await client.post(
#             url,
#             json=json
#         )
        
#         if response.status_code == 200:
#             # Extrai o JSON da resposta antes de acessar a chave
#             data = response.json()
#             results.append(data[id_register]) 
#         else:
#             print(f"Erro ao inserir na rota {url}: {response.text}")
            
#         return response.status_code
# ================================================================================

async def preload(jsons, url):
    print(f"Enviando lote de {len(jsons)} registros para {url}bulk ...")
    
    # Timeout generoso de 60s apenas para garantir que o banco grave tudo
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url + "bulk", json=jsons)
        
        if response.status_code == 200:
            print(f"Sucesso! {len(jsons)} inseridos.")
        else:
            print(f"Erro fatal no Bulk Insert: {response.text}")
            raise Exception(f"Falha ao popular o banco de dados!, {url + "bulk"}, {jsons}")


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
                    id_pedido = response.get("id_pedido", None)
                    id_entregador = response.get("id_entregador", None)
                    status = response.get("status", None)
                    # Tratamento de entregadores que foram alocados em um pedido (de desocupados para ocupados)
                    if item["user"] == "courier" and id_pedido and id_entregador:
                        async with entregadores_desocupados_lock:
                            posicao = entregadores_desocupados[id_entregador]["posicao"]
                            del entregadores_desocupados[id_entregador]
                        async with entregadores_ocupados_lock:
                            entregadores_ocupados[id_entregador] = {
                                "id_pedido": id_pedido,
                                "posicao": posicao,
                                "edge_idx": 0,
                                "rota_atual": "rota_restaurante",
                                "deslocamento_atual": [None, None, None]
                            }
                    elif item["user"] == "client" and status and id_pedido:
                        # Tratamento de mudanca de estados do pedido (simulando restaurante)
                        prob = random.random()
                        novo_status = None
                        if status == "CONFIRMED" and prob <= 0.95:
                            novo_status = "PREPARING"
                        elif status == "PREPARING" and prob <= 0.85:
                            novo_status = "READY_FOR_PICKUP"

                        if novo_status:
                            await queue.put({
                                "method": "PATCH",
                                "url": f"{URL}/pedidos/{id_pedido}/status",
                                "json": {"novo_status": novo_status}, 
                                "ritmo_idx": ritmo_idx
                            })
                            async with orders_lock:
                                orders[id_pedido]["status"] = novo_status


                elif method in ("POST", "PATCH"):
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
            

async def producer_order(queue, volume, duracao, ritmo_idx, clientes, restaurantes):
    start = time.perf_counter()
    orders_acum = 0

    while time.perf_counter() - start < duracao:
        # Produz pedidos seguindo uma distribuicao de poisson
        delta = random.expovariate(volume)
        await asyncio.sleep(delta)

        # Escolhe aleatoriamente um cliente e um restaurante
        id_cli = random.choice(clientes)
        id_res = random.choice(restaurantes)
        json = {"id_cliente": id_cli, "id_restaurante": id_res, "lista_itens": []}

        async with orders_lock:
            order_id = orders_acum
            orders[order_id] = {"id_cliente": id_cli, "id_restaurante": id_res, "status": "CONFIRMED"}
            orders_acum += 1
        
        await queue.put({
            "method": "POST",
            "url": f"{URL}/pedidos/",
            "json": json, 
            "ritmo_idx": ritmo_idx
        })


async def viewer_order(queue, volume, duracao, ritmo_idx):
    start = time.perf_counter()
    await asyncio.sleep(start + 1 - time.perf_counter())
    
    # Comeca no final do primeiro segundo e termina no final do ultimo (depois da criacao do ultimo pedido)
    while time.perf_counter() - start < duracao + 1:
        async with orders_lock:
            current_orders = dict(orders)
        
        if not current_orders:
            await asyncio.sleep(0.01)
            continue
        
        # Escolhe aleatoriamente um pedido
        order_id = random.choice(list(current_orders.keys()))

        # Produz visualizacoes de pedidos seguindo uma distribuicao de poisson
        delta = random.expovariate(volume)
        await asyncio.sleep(delta)
            
        await queue.put({
            "method": "GET",
            "url": f"{URL}/pedidos/{order_id}/acompanhamento",
            "params": {}, 
            "ritmo_idx": ritmo_idx,
            "user": "client"
        })

async def move_courier(id_entregador):
    # Considera a velocidade constante de 60Km/h = 1/600 metros/100ms
    def compute_new_desl(entregador):
        edge_idx = entregador["edge_idx"]
        rota_atual = entregador["rota_atual"]
        (lat, long), (new_lat, new_long), metros = entregador[rota_atual][edge_idx]
        dist_100ms = int(600 * metros + 0.5)
        lat_desl, long_desl = (new_lat - lat)/dist_100ms, (new_long - long)/dist_100ms
        entregador["deslocamento_atual"] = [lat_desl, long_desl, dist_100ms]
        entregador["posicao"] = lat, long

    async with entregadores_ocupados_lock:
        couriers = dict(entregadores_ocupados)

    entregador = couriers.get(id_entregador, None)
    if not entregador:
        return None, None, None
    elif not all(entregador["deslocamento_atual"]): # Inicia a rota ate o restaurante
        entregador["rota_atual"] = "rota_restaurante"
        entregador["edge_idx"] = 0
        compute_new_desl(entregador)

    mudanca_rota = False
    entregador["deslocamento_atual"][2] -= 1
    lat, long = entregador["posicao"]
    lat_desl, long_desl, dist_100ms = entregador["deslocamento_atual"]
    # Muda a aresta ao chegar ao final dela
    if dist_100ms <= 0:
        rota_atual = entregador["rota_atual"]
        if rota_atual == "rota_restaurante":
            # Espera ate o pedido estar pronto para ser coletado
            id_pedido = entregador.id_pedido
            async with orders_lock:
                status = orders[id_pedido]["status"]
            if status != "READY_FOR_PICKUP":
                return lat, long, None
            
            # Caso esteja pronto, coleta o pedido, atualiza a rota e o status do pedido
            entregador["rota_atual"] = "rota_cliente"
            entregador["edge_idx"] = 0
            async with orders_lock:
                orders[id_pedido]["status"] = "IN_TRANSIT"
            compute_new_desl(entregador)
            mudanca_rota = True
        elif rota_atual == "rota_cliente":
            entregador["rota_atual"] = None
            return None, None, None
        
    entregador["posicao"] = lat + lat_desl, long + long_desl 
    async with entregadores_ocupados_lock:
        entregadores_ocupados[id_entregador] = entregador

    return *entregador["posicao"], mudanca_rota


async def updater_courier(queue, volume, duracao, ritmo_idx):
    start = time.perf_counter()
    current_100ms = 0.0
    volume *= 3
    
    while time.perf_counter() - start < duracao:
        couriers = None
        async with entregadores_ocupados_lock:
            couriers = dict(entregadores_ocupados)
        
        if not couriers:
            await asyncio.sleep(0.01)
            continue

        # Cada entregador ocupado atualiza sua posicao ate o fim do pedido
        times = []
        while len(times) < volume:
            delta = random.expovariate(volume)
            if delta >= 1: continue
            times.append(delta)
        times = sorted(times)
        for id_entregador, each_time in zip(list(couriers.keys()), times):
            lat, long, mudanca_rota = move_courier(id_entregador)
            # Caso tenha acabado a corrida, desativa a sua alocacao e conclui o pedido
            if not lat or not long: 
                id_pedido = couriers[id_entregador]["id_pedido"]
                await queue.put({
                    "method": "POST",
                    "url": f"{URL}/alocacoes/{id_entregador}/desativar/{id_pedido}",
                    "json": {}, 
                    "ritmo_idx": ritmo_idx
                })
                await queue.put({
                    "method": "PATCH",
                    "url": f"{URL}/pedidos/{id_pedido}/status",
                    "json": {"novo_status": "DELIVERED"}, 
                    "ritmo_idx": ritmo_idx
                })
                async with orders_lock:
                    del orders[id_pedido]
                # Sai de ocupado e volta para desocupado
                async with entregadores_ocupados_lock:
                    posicao = entregadores_ocupados[id_entregador]["posicao"]
                    del entregadores_ocupados[id_entregador]
                async with entregadores_desocupados_lock:
                    entregadores_desocupados[id_entregador] = {"posicao": posicao}
            else:
                # Do contrario, so atualiza sua posicao
                json = {"latitude": lat, "latitude": long}
                await queue.put({
                    "method": "POST",
                    "url": f"{URL}/entregadores/{id_entregador}/posicao",
                    "json": json, 
                    "ritmo_idx": ritmo_idx
                })
                # Caso tenha coletado o pedido e esteja em direcao ao cliente, atualiza nas bases
                if mudanca_rota:
                    await queue.put({
                        "method": "PATCH",
                        "url": f"{URL}/pedidos/{id_pedido}/status",
                        "json": {"novo_status": "PICKED_UP"}, 
                        "ritmo_idx": ritmo_idx
                    })
                    await queue.put({
                        "method": "PATCH",
                        "url": f"{URL}/pedidos/{id_pedido}/status",
                        "json": {"novo_status": "IN_TRANSIT"}, 
                        "ritmo_idx": ritmo_idx
                    })
            delta = start + current_100ms + each_time - time.perf_counter()
            await asyncio.sleep(max(delta, 0))

        # Atualiza a cada 100ms
        current_100ms += 0.1
        delta = start + current_100ms - time.perf_counter()
        await asyncio.sleep(max(delta, 0))


async def viewer_courier(queue, volume, duracao, ritmo_idx):
    start = time.perf_counter()
    current_second = 0
    volume *= 3

    times = []
    while len(times) < volume:
        delta = random.expovariate(volume)
        if delta >= 1: continue
        times.append(delta)
    times = sorted(times)
    while time.perf_counter() - start < duracao:
        # Cada entregador desocupado fica esperando um pedido
        for each_time in times:
            couriers = None
            async with entregadores_desocupados_lock:
                couriers = list(entregadores_desocupados.keys())
            
            if not couriers:
                await asyncio.sleep(0.01)
                continue
            
            id_entregador = random.choice(couriers)

            delta = start + current_second + each_time - time.perf_counter()
            await asyncio.sleep(max(delta, 0))

            await queue.put({
                "method": "GET",
                "url": f"{URL}/alocacoes/{id_entregador}/acompanhamento",
                "params": {},
                "ritmo_idx": ritmo_idx,
                "user": "courier"
            })

        # Dorme ate o proximo segundo
        current_second += 1
        delta = start + current_second - time.perf_counter()
        await asyncio.sleep(max(delta, 0))
        


orders = {}
orders_lock = asyncio.Lock()
entregadores_desocupados = {}
entregadores_desocupados_lock = asyncio.Lock()
entregadores_ocupados = {}
entregadores_ocupados_lock = asyncio.Lock()

async def main():
    # Popular o sistema com dados basicos
    ids = {}
    data = gerar_dados_falsos( # clientes, restaurantes, entregadores
        numero_de_clientes=N_CLIENTES, 
        numero_de_restaurantes=N_RESTAURANTES, 
        numero_de_entregadores=N_ENTREGADORES
    )

    # No main.py essas taks possuem uma barra no final, mudei aqui
    tasks = [
        preload(jsons, URL + path)
        for jsons, path, id_register in zip(
            data,
            ["/clientes/", "/restaurantes/", "/entregadores/"],
            ["id_cliente", "id_restaurante", "id_entregador"]
        )
    ]
    await asyncio.gather(*tasks)
    clientes = list(range(1, N_CLIENTES+1))
    restaurantes = list(range(1, N_CLIENTES+1))
    async with entregadores_desocupados_lock:
        entregadores_desocupados = {
            id_entregador: {"posicao": [lat, long]} 
            for id_entregador, [lat, long] in zip(
                list(range(1, N_CLIENTES+1)), 
                [[v["latitude"], v["longitude"]] for v in data[2].values()]
            )
        }  
    
    # Cliente pode consultar o status do pedido (estado, entregador e posicao)
    # API deve responder em menos de 500ms no 95 percentil
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
            asyncio.create_task(viewer_order(queue, volume, duracao, idx)),
            asyncio.create_task(updater_courier(queue, volume, duracao, ritmo_idx)),
            asyncio.create_task(viewer_courier(queue, volume, duracao, ritmo_idx))
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
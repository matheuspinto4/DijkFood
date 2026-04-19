import asyncio
import httpx
import time
import statistics
import random

from fake_data import gerar_dados_falsos


# Define variaveis
N_CLIENTES     = 2#1000
N_RESTAURANTES = 10#50
N_ENTREGADORES = 4#3 * N_CLIENTES

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
        "duracao": 6#60
    },
    # {
    #     "volume": "PICO",
    #     "duracao": 2#20
    # },
    # {
    #     "volume": "EVENTO_ESPECIAL",
    #     "duracao": 4#40
    # },
    # {
    #     "volume": "OPERACAO_NORMAL",
    #     "duracao": 2#20
    # },
    # {
    #     "volume": "PICO",
    #     "duracao": 4#40
    # }
]


async def preload(jsons, url):
    print(f"Enviando lote de {len(jsons)} registros para {url}bulk ...")
    return 
    # Timeout generoso de 60s apenas para garantir que o banco grave tudo
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url + "bulk", json=jsons)
        
        if response.status_code == 200:
            print(f"Sucesso! {len(jsons)} inseridos.")
        else:
            print(f"Erro fatal no Bulk Insert: {response.text}")
            raise Exception(f"Falha ao popular o banco de dados!, {url}bulk, {jsons}")


async def requester(queue, results):
    def alloc_courier(entregador_dict, id_pedido, rota_restaurante, rota_cliente):
        entregador_dict["id_pedido"] = id_pedido
        entregador_dict["edge_idx"] = 0
        entregador_dict["rota_restaurante"] = rota_restaurante
        entregador_dict["rota_cliente"] = rota_cliente
        entregador_dict["rota_atual"] = "rota_restaurante"
        entregador_dict["deslocamento_atual"] = [*entregador_dict["posicao"], 0]

    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            start = time.perf_counter()
            try:
                if item["method"] == "GET":
                    response = await client.get(item["url"])
                    
                    # Tratamento de entregadores que foram alocados em um pedido (de desocupados para ocupados)
                    id_pedido = response.get("id_pedido", None)
                    id_entregador = response.get("id_entregador", None)
                    rota_restaurante = response.get("rota_restaurante", None)
                    rota_cliente = response.get("rota_cliente", None)
                    if item["user"] == "courier" and id_pedido and id_entregador:
                        async with entregadores_desocupados_lock:
                            entregador_dict = entregadores_desocupados[id_entregador]
                            del entregadores_desocupados[id_entregador]
                        async with entregadores_ocupados_lock:
                            entregadores_ocupados[id_entregador] = alloc_courier(entregador_dict, id_pedido, rota_restaurante, rota_cliente)

                elif item["method"] in ("POST", "PATCH"):
                    response = await client.post(
                        item["url"],
                        json=item["json"]
                    )
                    # Pedidos coletados pelo entregador ja vao direto para em transito
                    if item["user"] == "courier" and item["json"].get("novo_status", "") == "":
                        await queue.put({
                            "method": "PATCH",
                            "url": item["url"],
                            "json": {"novo_status": "PICKED_UP"}, 
                            "ritmo_idx": item["ritmo_idx"],
                            "user": "courier"
                        })
                latency = time.perf_counter() - start

                results.append({
                    "status": "foi", # response.status_code,
                    "latency": latency,
                    "ritmo_idx": item["ritmo_idx"],
                    "method": item["method"],
                    "user": item["user"]
                })

            except Exception as e:
                latency = time.perf_counter() - start

                results.append({
                    "status": "error",
                    "latency": latency,
                    "error": str(e),
                    "ritmo_idx": item["ritmo_idx"],
                    "method": item["method"],
                    "user": item["user"]
                })

            queue.task_done()


async def restaurant_updater(queue, volume, duracao, ritmo_idx):
    volume = max(volume // 6, 1)
    start = time.perf_counter()
    

    while time.perf_counter() - start < duracao:
        # Produz atualizacoes de pedidos seguindo uma distribuicao de poisson
        current_start = time.perf_counter()
        delta = random.expovariate(volume)
        
        # Define qual atualizacao sera feita
        status = "CONFIRMED"
        novo_status = "PREPARING"
        if random.random() <= 0.1:
            status = "PREPARING"
            novo_status = "READY_FOR_PICKUP"
        
        # Atualiza o primeiro pedido com o status selecionado
        try:
            async with orders_lock:
                current_orders = dict(orders)
            id_pedido = next(pedido_num for pedido_num, pedido in current_orders.items() if pedido["status"] == status)
        except Exception as e:
            await asyncio.sleep(0.01)
            continue
        
        await queue.put({
            "method": "PATCH",
            "url": f"{URL}/pedidos/{id_pedido}/status",
            "json": {"novo_status": novo_status}, 
            "ritmo_idx": ritmo_idx,
            "user": "restaurant"
        })
        async with orders_lock:
            orders[id_pedido]["status"] = novo_status
            
        # Passa o restante do tempo reservado para a requisicao
        delta_sleep = current_start + delta - time.perf_counter()
        await asyncio.sleep(max(delta_sleep, 0))


async def move_courier(id_entregador):
    # Considera a velocidade constante de 60Km/h = 10/6 metro a cada 100ms
    def compute_new_desl(entregador):
        edge_idx = entregador["edge_idx"]
        rota_atual = entregador["rota_atual"]
        # So atualiza se tiver mais arestas a percorrer
        if edge_idx >= len(entregador[rota_atual]): return 1
        
        (lat, long), (new_lat, new_long), metros = entregador[rota_atual][edge_idx]
        dist_100ms = int(3/5 * metros + 0.5)
        lat_desl, long_desl = (new_lat - lat)/dist_100ms, (new_long - long)/dist_100ms
        entregador["deslocamento_atual"] = [lat_desl, long_desl, dist_100ms]
        entregador["posicao"] = lat, long
        entregador["edge_idx"] += 1
        return 0

    async with entregadores_ocupados_lock:
        couriers = dict(entregadores_ocupados)

    entregador = couriers.get(id_entregador, None)
    if not entregador:
        return None, None, None
    elif entregador["deslocamento_atual"][2] <= 0: # Atualiza a aresta atual
        fim_rota = compute_new_desl(entregador)
        
    lat, long = entregador["posicao"]
    lat_desl, long_desl, dist_100ms = entregador["deslocamento_atual"] 
    # Da um passo rumo ao final da aresta
    if dist_100ms > 0:   
        entregador["posicao"] = lat + lat_desl, long + long_desl 
        entregador["deslocamento_atual"][2] = dist_100ms - 1
        mudanca_rota = False
    elif fim_rota:
        rota_atual = entregador["rota_atual"]
        if rota_atual == "rota_restaurante":
            # Espera ate o pedido estar pronto para ser coletado
            id_pedido = entregador["id_pedido"]
            async with orders_lock:
                status = orders[id_pedido]["status"]
            if status != "READY_FOR_PICKUP":
                return lat, long, None
            
            # Caso esteja pronto, coleta o pedido, atualiza a rota e o status do pedido
            entregador["rota_atual"] = "rota_cliente"
            entregador["edge_idx"] = 0
            async with orders_lock:
                orders[id_pedido]["status"] = "IN_TRANSIT"
            mudanca_rota = True
        elif rota_atual == "rota_cliente":
            return None, None, None
    
    async with entregadores_ocupados_lock:
        entregadores_ocupados[id_entregador] = entregador

    return *entregador["posicao"], mudanca_rota


async def updater_courier(queue, volume, duracao, ritmo_idx):
    def desalloc_courier(entregador_dict):
        entregador_dict["id_pedido"] = None
        entregador_dict["edge_idx"] = None
        entregador_dict["rota_restaurante"] = None
        entregador_dict["rota_cliente"] = None
        entregador_dict["rota_atual"] = None
        entregador_dict["deslocamento_atual"] = None
        
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
            lat, long, mudanca_rota = await move_courier(id_entregador)
            id_pedido = couriers[id_entregador]["id_pedido"]
            # Caso tenha acabado a corrida, desativa a sua alocacao e conclui o pedido
            if not lat or not long: 
                await queue.put({
                    "method": "POST",
                    "url": f"{URL}/alocacoes/{id_entregador}/desativar/{id_pedido}",
                    "json": {}, 
                    "ritmo_idx": ritmo_idx,
                    "user": "courier"
                })
                await queue.put({
                    "method": "PATCH",
                    "url": f"{URL}/pedidos/{id_pedido}/status",
                    "json": {"novo_status": "DELIVERED"}, 
                    "ritmo_idx": ritmo_idx,
                    "user": "courier"
                })
                async with orders_lock:
                    del orders[id_pedido]
                # Sai de ocupado e volta para desocupado
                async with entregadores_ocupados_lock:
                    entregador_dict = entregadores_ocupados[id_entregador]
                    del entregadores_ocupados[id_entregador]
                async with entregadores_desocupados_lock:
                    entregadores_desocupados[id_entregador] = desalloc_courier(entregador_dict)
            else:
                # Do contrario, so atualiza sua posicao
                json = {"latitude": lat, "longitude": long}
                await queue.put({
                    "method": "POST",
                    "url": f"{URL}/entregadores/{id_entregador}/posicao",
                    "json": json, 
                    "ritmo_idx": ritmo_idx,
                    "user": "courier"
                })
                # Caso tenha coletado o pedido e esteja em direcao ao cliente, atualiza nas bases
                if mudanca_rota:
                    await queue.put({
                        "method": "PATCH",
                        "url": f"{URL}/pedidos/{id_pedido}/status",
                        "json": {"novo_status": "PICKED_UP"}, 
                        "ritmo_idx": ritmo_idx,
                        "user": "courier"
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
       


async def producer_order(queue, volume, duracao, ritmo_idx, clientes, restaurantes):
    start = time.perf_counter()
    orders_acum = 0#1

    while time.perf_counter() - start < duracao:
        # Produz pedidos seguindo uma distribuicao de poisson
        current_start = time.perf_counter()
        delta = random.expovariate(volume)

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
            "ritmo_idx": ritmo_idx,
            "user": "client"
        })
        
        # Passa o restante do tempo reservado para a requisicao
        delta_sleep = current_start + delta - time.perf_counter()
        await asyncio.sleep(max(delta_sleep, 0))


async def viewer_order(queue, volume, duracao, ritmo_idx):
    start = time.perf_counter()
    await asyncio.sleep(start + 1 - time.perf_counter())
    
    # Comeca no final do primeiro segundo e termina no final do ultimo (depois da criacao do ultimo pedido)
    while time.perf_counter() - start < duracao + 1:
        # Produz pedidos seguindo uma distribuicao de poisson
        current_start = time.perf_counter()
        delta = random.expovariate(volume)
        
        try:
            async with orders_lock:
                current_orders = dict(orders)
            # Escolhe aleatoriamente um pedido
            order_id = random.choice(list(current_orders.keys()))
        except Exception as e:
            await asyncio.sleep(0.01)
            continue
          
        await queue.put({
            "method": "GET",
            "url": f"{URL}/pedidos/{order_id}/acompanhamento",
            "params": {}, 
            "ritmo_idx": ritmo_idx,
            "user": "client"
        })
        
        # Passa o restante do tempo reservado para a requisicao
        delta_sleep = current_start + delta - time.perf_counter()
        await asyncio.sleep(max(delta_sleep, 0))
       
        

orders = {
    0: {"id_cliente": 1, "id_restaurante": 1, "status": "CONFIRMED"}
}
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
        for jsons, path in zip(
            data,
            ["/clientes/", "/restaurantes/", "/entregadores/"]
        )
    ]
    await asyncio.gather(*tasks)
    clientes = list(range(1, N_CLIENTES+1))
    restaurantes = list(range(1, N_CLIENTES+1))
    async with entregadores_desocupados_lock:
        for id_entregador, [lat, long] in zip(
            list(range(1, N_ENTREGADORES+1)), 
            [[v["latitude"], v["longitude"]] for v in data[2]]
        ): 
            entregadores_desocupados[id_entregador] = {
                "id_pedido": None,
                "posicao": [lat, long],
                "edge_idx": None,
                "rota_restaurante": None,
                "rota_cliente": None,
                "rota_atual": None,
                "deslocamento_atual": None
            }
    
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
            asyncio.create_task(updater_courier(queue, volume, duracao, idx)),
            asyncio.create_task(viewer_courier(queue, volume, duracao, idx)),
            asyncio.create_task(restaurant_updater(queue, volume, duracao, idx))
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
    latencias = {i: {"PATCH": [], "POST": [], "GET": []} for i in range(len(RITMO_EXEC))}
    for r in results:
        ritmo_idx = r["ritmo_idx"]
        latency = r["latency"]
        method = r["method"]
        latencias[ritmo_idx][method].append(latency)

    # Metricas - API deve responder em menos de 500ms no 95 percentil
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
        ritmo_dura = RITMO_EXEC[ritmo_idx]["duracao"]
        prefixos = [f"{ritmo_idx+1}. {ritmo_name}", f"   {ritmo_dura} segundos", ""]
        for prefix, sufix, latencia in zip(prefixos, ritmo_latencia.keys(), ritmo_latencia.values()):
            print(f"| {prefix:<19}", f"{sufix:>9}|", *[f"{round(f(latencia), 4) if latencia != [] else '':^10}|" for f in metrics.values()])
        print(seccion)
    
    
if __name__ == "__main__":
    asyncio.run(main())
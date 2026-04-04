# Define variaveis
N_CLIENTES      = 1000
N_RESTAURANTES  = 50
N_ENTREGADORES  = 3 * N_CLIENTES
N_MILISSEGUNDOS = 60 * 3
N_DDB_ITEMS     = 50000
VOLUMES = {
    "OPERACAO_NORMAL": 10,
    "PICO": 50,
    "EVENTO_ESPECIAL": 200
}


# Popular o sistema com dados basicos


# Executar as operacoes por segundo
# Entregador reporta posicao (1000 ms)
# Cliente pode consultar o status do pedido (estado, entregador e posicao)
# Adm pode consultar histórico de pedidos de um cliente (Ordem cronologica)
# Adm pode consultar histórico de eventos de um pedido (Ordem cronologica)
# API deve responder em menos de 500ms no 95 percentil
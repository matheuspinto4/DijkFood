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
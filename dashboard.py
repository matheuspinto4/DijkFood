import streamlit as st
import subprocess
import requests
import time
import os
import sys
import pandas as pd
from streamlit_autorefresh import st_autorefresh

INTERVALO_SEGUNDOS = 3

@st.cache_resource
def get_api_gateway_url():
    result = subprocess.run(
        ["terraform", "output", "-raw", "api_gateway_url"],
        capture_output=True, text=True, cwd="terraform"
    )
    if result.returncode != 0 or not result.stdout.strip():
        print("Erro: api_gateway_url não encontrado nos outputs do Terraform.")
        print("Rode 'terraform apply' primeiro.")
        sys.exit(1)
    return result.stdout.strip().rstrip("/")


def buscar(url, path):
    try:
        r = requests.get(f"{url}{path}", timeout=5)
        if r.status_code == 200:
            return r.json()
        return {"erro": f"HTTP {r.status_code}"}
    except requests.exceptions.ConnectionError:
        return {"erro": "sem conexão"}
    except requests.exceptions.Timeout:
        return {"erro": "timeout"}
    except Exception as e:
        return {"erro": str(e)}


API_URL = get_api_gateway_url()


st_autorefresh(
    interval=3000,
    key="refresh"
)

orders = buscar(API_URL, "/metrics/orders")
entregadores = buscar(API_URL, "/metrics/entregadores")
throughput = buscar(API_URL, "/metrics/throughput")

# print(orders)
# print(entregadores)
# print(throughput)

st.title("DijkFood Dashboard")

# st.metric(
#     "Pedidos Totais",
#     1234
# )

# st.metric(
#     "Entregadores Ativos",
#     42
# )

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Pedidos",
        orders.get("quantidade", 0)
    )

with col2:
    st.metric(
        "Entregadores em pedidos",
        entregadores.get("ativos", 0)
    )

with col3:
    st.metric(
        "Pedidos/min",
        throughput.get("pedidos_ultimo_minuto", 0)
    )

with col4:
    st.metric(
        "Alocações",
        throughput.get("alocacoes_total", 0)
    )

df_status = pd.DataFrame(
    orders["por_status"].items(),
    columns=["Status", "Quantidade"]
)
df = pd.DataFrame(
    orders["orders"]
).T
st.bar_chart(
    df_status.set_index("Status")
)
# print(df)
# print(df_status)

# print(orders["itens"])
# print(df_status)
# print(df.head())
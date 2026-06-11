import streamlit as st
import altair as alt
import subprocess
import requests
import time
import os
import sys
import pandas as pd
from streamlit_autorefresh import st_autorefresh

INTERVALO_SEGUNDOS = 3
STATES = ["CONFIRMED", "PREPARING", "READY_FOR_PICKUP", "PICKED_UP", "IN_TRANSIT", "DELIVERED"]

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

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Pedidos Ativos",
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
print(df.shape)


chart_orders = alt.Chart(df_status).mark_bar().encode(
    x=alt.X('Status', sort=None), # O argumento sort=None impede a ordenação
    y='Quantidade'
)
st.altair_chart(chart_orders, width='stretch')


timers = df[[s for s in STATES if s != "DELIVERED" and s in df.columns]].mean().reset_index()
timers.columns = ["Status", "Tempo (seg)"]
chart_timers = alt.Chart(timers).mark_bar().encode(
    x=alt.X('Status', sort=None), # O argumento sort=None impede a ordenação
    y='Tempo (seg)'
)
st.altair_chart(chart_timers, width='stretch')



# print(df_status)

# print(orders["itens"])
# print(df_status)
# if "regiao" in df.columns:
#     print(df.tail(2))#[~df["regiao"].isna()].head(1))
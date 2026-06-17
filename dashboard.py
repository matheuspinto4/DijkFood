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
restaurantes = buscar(API_URL, "/metrics/restaurantes")

print(restaurantes)

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
        "Total Pedidos",
        orders.get("total", 0)
    )

with col3:
    st.metric(
        "Entregadores em pedidos",
        entregadores.get("ativos", 0)
    )

busy_time = entregadores.get("busy_time", 0)
idle_time = entregadores.get("idle_time", 0)
total_time = max(busy_time + idle_time, 1)
with col4:
    st.metric(
        "Ociosidade (%)",
        round(100 * idle_time / total_time, 2)
    )

col5, col6 = st.columns(2)
with col5:
    st.metric("Utilização da capacidade", f'{restaurantes.get("utilizacao_capacidade", 0)}%')
with col6:
    st.metric("Vazão vs histórico do horário", f'{restaurantes.get("aderencia_horaria", 0)}%')


df_status = pd.DataFrame(
    orders["por_status"].items(),
    columns=["Status", "Quantidade"]
)
chart_status = alt.Chart(df_status).mark_bar().encode(
    x=alt.X('Status', sort=None), # O argumento sort=None impede a ordenação
    y='Quantidade'
)
st.altair_chart(chart_status, width='stretch')


df_mean_duration = pd.DataFrame(
    orders["duracao_media_por_status"].items(),
    columns=["Status", "Duração média"]
)
chart_mean_duration = alt.Chart(df_mean_duration).mark_bar().encode(
    x=alt.X('Status', sort=None), # O argumento sort=None impede a ordenação
    y='Duração média'
)
st.altair_chart(chart_mean_duration, width='stretch')


df_regiao = pd.DataFrame(
    orders["por_regiao"].items(),
    columns=["Região", "Quantidade"]
)
chart_regiao = alt.Chart(df_regiao).mark_bar().encode(
    x=alt.X('Região', sort=None), # O argumento sort=None impede a ordenação
    y='Quantidade'
)
st.altair_chart(chart_regiao, width='stretch')


df_weekday = pd.DataFrame(
    orders["por_dia_semana"].items(),
    columns=["Dia/Hora", "Quantidade"]
)
chart_weekday = alt.Chart(df_weekday).mark_bar().encode(
    x=alt.X('Dia/Hora'), # O argumento sort=None impede a ordenação
    y='Quantidade'
)
st.altair_chart(chart_weekday, width='stretch')


# df_rest_top10 = pd.DataFrame(
#     orders["top_10_restaurants"],#.items(),
#     columns=["Restaurante", "Quantidade"]
# )
df_rest_top10 = pd.DataFrame(
    [
        {
            "Restaurante": r["id"],
            "Quantidade": r["pedidos"]
        }
        for r in orders["top_10_restaurants"]
    ]
)
chart_rest_top10 = alt.Chart(df_rest_top10).mark_bar().encode(
    x=alt.X('Restaurante', sort=None), # O argumento sort=None impede a ordenação
    y='Quantidade'
)
st.altair_chart(chart_rest_top10, width='stretch')

df_duracao_hist = pd.DataFrame(
    orders["histograma_duracao"].items(),
    columns=["Duração", "Quantidade"]
)
chart_duracao_hist = alt.Chart(df_duracao_hist).mark_bar().encode(
    x=alt.X('Duração', sort=None), # O argumento sort=None impede a ordenação
    y='Quantidade'
)
st.altair_chart(chart_duracao_hist, width='stretch')

# timers = df[[s for s in STATES if s != "DELIVERED" and s in df.columns]].mean().reset_index()
# timers.columns = ["Status", "Tempo (seg)"]
# chart_timers = alt.Chart(timers).mark_bar().encode(
#     x=alt.X('Status', sort=None), # O argumento sort=None impede a ordenação
#     y='Tempo (seg)'
# )
# st.altair_chart(chart_timers, width='stretch')



# print(df_status)

# print(orders["itens"])
# print(df_status)
# if "regiao" in df.columns:
#     print(df.tail(2))#[~df["regiao"].isna()].head(1))
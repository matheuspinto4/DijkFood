import streamlit as st
import pandas as pd
import redshift_connector
import boto3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import time

st.set_page_config(page_title="DijkFood", layout="wide")
st.title("DijkFood - Dashboard Analítico")
st.markdown("Monitoramento **Contínuo** (Redshift Spectrum) - Atualização a cada 3 segundos.")

def get_redshift_connection():
    client = boto3.client('redshift', region_name='us-east-1')
    clusters = client.describe_clusters()['Clusters']
    try:
        meu_cluster = next(c for c in clusters if c['ClusterIdentifier'].startswith('dijkfood-analytics-cluster'))
        return redshift_connector.connect(
            host=meu_cluster['Endpoint']['Address'], database='dev', user='awsuser', password='SenhaForte2026!', port=meu_cluster['Endpoint']['Port']
        )
    except StopIteration:
        st.error("Cluster Redshift não encontrado.")
        st.stop()

def load_events():
    conn = get_redshift_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                id_pedido, 
                id_cliente, 
                nome_restaurante, 
                status, 
                CAST(timestamp AS TIMESTAMP) as event_time,
                latitude_cliente as lat, 
                longitude_cliente as lon
            FROM spectrum_schema.pedidos_stream
            WHERE timestamp IS NOT NULL
        """)
        
        df = pd.DataFrame(cursor.fetchall(), columns=[
            'id_pedido', 'id_cliente', 'nome_restaurante', 'status', 'event_time', 'lat', 'lon'
        ])
        return df
    finally:
        cursor.close()
        conn.close()

try:
    df_events = load_events()

    if df_events.empty:
        st.info("Nenhum evento registrado no S3 com timestamp até o momento. Aguardando dados...")
    else:
        df_events['event_time'] = pd.to_datetime(df_events['event_time'], errors='coerce')
        df_events = df_events.dropna(subset=['event_time'])
        df_events['nome_restaurante'] = df_events['nome_restaurante'].fillna('Desconhecido')
        
        df_pedidos = df_events.sort_values('event_time').groupby('id_pedido').first().reset_index()

        col1, col2, col3 = st.columns(3)
        col1.metric("Total de Pedidos Únicos", len(df_pedidos))
        col2.metric("Eventos Processados", len(df_events))
        col3.metric("Última Atualização", pd.Timestamp.now().strftime("%H:%M:%S"))
        
        st.divider()

        # Linha 1
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Volume de Pedidos no Tempo")
            df_pedidos['minuto'] = df_pedidos['event_time'].dt.floor('Min')
            volume_tempo = df_pedidos.groupby('minuto').size()
            
            fig_vol, ax_vol = plt.subplots(figsize=(8, 4))
            ax_vol.plot(volume_tempo.index, volume_tempo.values, color='#ff4b4b', linewidth=2)
            
            ax_vol.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            ax_vol.tick_params(axis='x', rotation=45)
            
            
            ax_vol.set_xlabel("Horário do Pedido")
            ax_vol.set_ylabel("Quantidade de Pedidos")
            
            ax_vol.spines['top'].set_visible(False)
            ax_vol.spines['right'].set_visible(False)
            st.pyplot(fig_vol)

        with c2:
            st.subheader("Top 10 Restaurantes (Volume)")
            top10 = df_pedidos['nome_restaurante'].value_counts().nlargest(10)
            
            fig_top, ax_top = plt.subplots(figsize=(8, 4))
            posicoes = range(len(top10)) 
            
            ax_top.bar(posicoes, top10.values, color='#ff4b4b')
            ax_top.set_xticks(posicoes)
            ax_top.set_xticklabels(top10.index, rotation=45, ha='right')
            
            
            ax_top.set_xlabel("Restaurante")
            ax_top.set_ylabel("Total de Pedidos")
            
            ax_top.spines['top'].set_visible(False)
            ax_top.spines['right'].set_visible(False)
            st.pyplot(fig_top)

        # Linha 2
        c3, c4 = st.columns(2)
        with c3:
            st.subheader("Tempo Médio no Estado (Segundos)")
            df_sorted = df_events.sort_values(['id_pedido', 'event_time'])
            df_sorted['tempo_no_estado'] = df_sorted.groupby('id_pedido')['event_time'].diff().dt.total_seconds()
            tempo_medio = df_sorted.groupby('status')['tempo_no_estado'].mean().dropna()
            
            status_map = {s: i for i, s in enumerate(tempo_medio.index)}
            x_ints = [status_map[s] for s in tempo_medio.index]
            
            fig_state, ax_state = plt.subplots(figsize=(8, 4))
            ax_state.bar(x_ints, tempo_medio.values, color='#ff4b4b')
            ax_state.set_xticks(x_ints)
            ax_state.set_xticklabels(tempo_medio.index, rotation=45, ha='right')
            
            
            ax_state.set_xlabel("Status do Pedido")
            ax_state.set_ylabel("Tempo Médio (Segundos)")
            
            ax_state.spines['top'].set_visible(False)
            ax_state.spines['right'].set_visible(False)
            st.pyplot(fig_state)

        with c4:
            st.subheader("Histograma do Tempo Total de Entrega")
            df_delivered = df_events[df_events['status'] == 'DELIVERED'][['id_pedido', 'event_time']].rename(columns={'event_time': 'end_time'})
            df_start = df_pedidos[['id_pedido', 'event_time']].rename(columns={'event_time': 'start_time'})
            df_total_time = pd.merge(df_delivered, df_start, on='id_pedido')
            df_total_time['total_time_m'] = (df_total_time['end_time'] - df_total_time['start_time']).dt.total_seconds() / 60.0
            
            if not df_total_time.empty:
                fig_hist, ax_hist = plt.subplots(figsize=(8, 4))
                ax_hist.hist(df_total_time['total_time_m'], bins=20, color='#ff4b4b', edgecolor='white')
                ax_hist.tick_params(axis='x', rotation=0)
                
                
                ax_hist.set_xlabel("Tempo Total de Entrega (Minutos)")
                ax_hist.set_ylabel("Frequência (Número de Pedidos)")
                
                ax_hist.spines['top'].set_visible(False)
                ax_hist.spines['right'].set_visible(False)
                st.pyplot(fig_hist)
            else:
                st.info("Nenhum pedido atingiu o status DELIVERED ainda.")

        # Linha 3
        c5, c6 = st.columns(2)
        with c5:
            st.subheader("Demanda por Horário/Dia")
            df_pedidos['hora'] = df_pedidos['event_time'].dt.hour.astype(int)
            df_pedidos['dia_semana'] = df_pedidos['event_time'].dt.dayofweek.astype(int)
            heatmap_data = df_pedidos.groupby(['dia_semana', 'hora']).size().unstack(fill_value=0)
            
            if not heatmap_data.empty:
                fig_heat, ax_heat = plt.subplots(figsize=(8, 4))
                sns.heatmap(heatmap_data, ax=ax_heat, cmap="Reds", annot=True, fmt="d", cbar=False)
                ax_heat.set_yticklabels(ax_heat.get_yticklabels(), rotation=0)
                ax_heat.set_xticklabels(ax_heat.get_xticklabels(), rotation=0)
                
                ax_heat.set_xlabel("Hora do Dia")
                ax_heat.set_ylabel("Dia da Semana")
                
                st.pyplot(fig_heat)
                
        with c6:
            st.subheader("Distribuição por Região")
            map_data = df_pedidos[['lat', 'lon']].dropna()
            if not map_data.empty:
                st.map(map_data, color='#ff4b4b', zoom=11)
            else:
                st.info("Coordenadas indisponíveis no momento.")

    time.sleep(3)
    st.rerun()

except Exception as e:
    st.error(f"Erro ao processar o dashboard: {e}")
    time.sleep(3)
    st.rerun()
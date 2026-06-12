import streamlit as st
import pandas as pd
import redshift_connector
import boto3
import matplotlib.pyplot as plt

# ---------------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# ---------------------------------------------------------
st.set_page_config(page_title="DijkFood | Live Analytics", layout="wide", page_icon="🍔")
st.title("🍔 DijkFood - Central de Inteligência Analítica")
st.markdown("Monitoramento em tempo real do volume de transações e comportamento da rede.")

# ---------------------------------------------------------
# CONEXÃO COM O REDSHIFT
# ---------------------------------------------------------
@st.cache_resource
def get_redshift_connection():
    client = boto3.client('redshift', region_name='us-east-1')
    clusters = client.describe_clusters()['Clusters']
    
    try:
        meu_cluster = next(c for c in clusters if c['ClusterIdentifier'].startswith('dijkfood-analytics-cluster'))
        endpoint = meu_cluster['Endpoint']['Address']
        port = meu_cluster['Endpoint']['Port']
    except StopIteration:
        st.error("Cluster Redshift não encontrado. Verifique se a infraestrutura está ativa.")
        st.stop()

    return redshift_connector.connect(
        host=endpoint, database='dev', user='awsuser', password='SenhaForte2026!', port=port
    )

# ---------------------------------------------------------
# EXTRAÇÃO DE DADOS
# ---------------------------------------------------------
@st.cache_data(ttl=60)
def load_data():
    conn = get_redshift_connection()
    cursor = conn.cursor()
    
    # 1. Restaurantes (A CORREÇÃO ESTÁ AQUI: Filtrar NULL direto no banco)
    cursor.execute("""
        SELECT id_restaurante, COUNT(id_pedido) as total_pedidos
        FROM pedidos 
        WHERE id_restaurante IS NOT NULL
        GROUP BY id_restaurante 
        ORDER BY total_pedidos DESC 
        LIMIT 10;
    """)
    df_restaurantes = pd.DataFrame(cursor.fetchall(), columns=['id_restaurante', 'total_pedidos'])
    
    # 2. Status (Contando os eventos dos Workers)
    cursor.execute("""
        SELECT status, COUNT(id_pedido) as total
        FROM pedidos
        WHERE status IS NOT NULL
        GROUP BY status;
    """)
    df_status = pd.DataFrame(cursor.fetchall(), columns=['status', 'total'])
    
    # 3. MODO DIAGNÓSTICO: Buscar o JSON Bruto (Array de SUPER)
    cursor.execute("SELECT dado FROM pedidos_raw LIMIT 10;")
    raw_data = cursor.fetchall()
    df_raw = pd.DataFrame([str(r[0]) for r in raw_data], columns=['JSON_Bruto_do_Kinesis'])
    
    cursor.close()
    return df_restaurantes, df_status, df_raw

# ---------------------------------------------------------
# RENDERIZAÇÃO DO DASHBOARD
# ---------------------------------------------------------
try:
    with st.spinner('Sincronizando com o Redshift...'):
        df_restaurantes, df_status, df_raw = load_data()

    # Como um 'pedido' no Redshift atualmente é um 'evento de log', somamos os status
    total_eventos = df_status['total'].sum() if not df_status.empty else 0
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Eventos Processados (Worker)", int(total_eventos))
    col2.metric("Restaurantes Ativos", len(df_restaurantes))
    col3.metric("Última Atualização", pd.Timestamp.now().strftime("%H:%M:%S"))
    
    st.divider()

    col_grafico1, col_grafico2 = st.columns(2)

    with col_grafico1:
        st.subheader("Top 10 Restaurantes (Volume)")
        
        # Proteção extra: Só desenha o gráfico se existirem restaurantes na base
        if not df_restaurantes.empty:
            df_restaurantes['id_restaurante'] = df_restaurantes['id_restaurante'].astype(int)
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.bar(df_restaurantes['id_restaurante'], df_restaurantes['total_pedidos'], color='#ff4b4b')
            ax.set_xlabel("Identificador do Restaurante", fontsize=10)
            ax.set_ylabel("Quantidade", fontsize=10)
            
            # Trava as labels como inteiros na horizontal
            ax.set_xticks(df_restaurantes['id_restaurante'])
            ax.set_xticklabels(df_restaurantes['id_restaurante'], rotation=0)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            st.pyplot(fig)
        else:
            # Caso os eventos de criação da API usem chaves diferentes, este aviso aparecerá
            st.info("Nenhum evento contendo a chave exata 'id_restaurante' foi encontrado no Data Lake.")

    with col_grafico2:
        st.subheader("Distribuição de Status (Workers)")
        st.dataframe(df_status, use_container_width=True)

    st.write("---")
    
    # Aba de Engenharia Reversa
    with st.expander("🛠️ Modo de Depuração: Inspecionar JSON Bruto"):
        st.write("Verifique como a API nomeou as chaves de criação do pedido. Se a API usar 'restaurante_id' em vez de 'id_restaurante', basta ajustar o script ELT do Redshift depois!")
        st.dataframe(df_raw, use_container_width=True)

    if st.button("Forçar Sincronização de Novos Dados"):
        st.cache_data.clear()
        st.rerun()

except Exception as e:
    st.error(f"Erro ao processar o dashboard: {e}")
import boto3
import redshift_connector
import subprocess

DB_NAME = 'dev'
MASTER_USER = 'awsuser'
MASTER_PASS = 'SenhaForte2026!'

def get_terraform_output(key):
    """Lê saídas do Terraform"""
    result = subprocess.run(
        ["terraform", "output", "-raw", key],
        capture_output=True, text=True, cwd="terraform"
    )
    return result.stdout.strip()

def main():
    client = boto3.client('redshift', region_name='us-east-1')
    sts = boto3.client('sts')
    lab_role_arn = f"arn:aws:iam::{sts.get_caller_identity()['Account']}:role/LabRole"

    # 1. Procura o cluster existente
    clusters = client.describe_clusters()['Clusters']
    try:
        meu_cluster = next(c for c in clusters if c['ClusterIdentifier'].startswith('dijkfood-analytics-cluster'))
        endpoint = meu_cluster['Endpoint']['Address']
        port = meu_cluster['Endpoint']['Port']
    except StopIteration:
        print("[!] Nenhum cluster Redshift ativo encontrado.")
        return

    print(f"[*] Cluster encontrado: {meu_cluster['ClusterIdentifier']}")
    print(f"[*] Conectando em {endpoint}:{port} ...")

    try:
        conn = redshift_connector.connect(
            host=endpoint, database=DB_NAME, user=MASTER_USER, password=MASTER_PASS, port=port
        )
        cursor = conn.cursor()

        # ---------------------------------------------------------
        # ETAPA 1: EXTRACT & LOAD (O Caminho Seguro - noshred)
        # ---------------------------------------------------------
        print("[*] ETAPA 1: Criando Staging Table (pedidos_raw)...")
        cursor.execute("DROP TABLE IF EXISTS pedidos;")
        cursor.execute("DROP TABLE IF EXISTS pedidos_raw;")
        
        # Uma tabela com uma única coluna que vai engolir todo o JSON sem questionar
        cursor.execute("CREATE TABLE pedidos_raw (dado SUPER);")
        conn.commit()

        NOME_DO_BUCKET_S3 = get_terraform_output("datalake_bucket_name")
        print(f"[*] Importando dados do S3 em modo Bruto (Bypassing Redshift bug)...")

        cursor.execute(f"""
            COPY pedidos_raw 
            FROM 's3://{NOME_DO_BUCKET_S3}/' 
            IAM_ROLE '{lab_role_arn}' 
            FORMAT AS JSON 'noshred' 
            REGION 'us-east-1'; 
        """) 
        conn.commit()
        print("[*] Todos os arquivos JSON foram sugados para dentro do Redshift!")

        # ---------------------------------------------------------
        # ETAPA 2: TRANSFORM (Desempacotando via SQL)
        # ---------------------------------------------------------
        print("[*] ETAPA 2: Estruturando os dados analíticos (ELT)...")
        
        cursor.execute("""
            CREATE TABLE pedidos (
                id_pedido INTEGER distkey,
                id_cliente INTEGER,
                id_restaurante INTEGER,
                id_entregador INTEGER,
                lista_itens SUPER,
                status VARCHAR(30),
                data DATE,
                horario TIME sortkey
            )
        """)
        
        # O Redshift permite navegação por ponto (dado.chave) para ler dentro da coluna SUPER
        cursor.execute("""
            INSERT INTO pedidos 
            SELECT 
                dado.id_pedido::INTEGER,
                dado.id_cliente::INTEGER,
                dado.id_restaurante::INTEGER,
                dado.id_entregador::INTEGER,
                dado.lista_itens,
                dado.status::VARCHAR,
                dado.data::VARCHAR::DATE,
                dado.horario::VARCHAR::TIME
            FROM pedidos_raw
            WHERE dado.id_pedido IS NOT NULL;
        """)
        conn.commit()

        # ---------------------------------------------------------
        # PROVA FINAL
        # ---------------------------------------------------------
        cursor.execute("SELECT COUNT(*) FROM pedidos;")
        total = cursor.fetchone()[0]
        print(f"\n✅ SUCESSO! Total de pedidos empacotados perfeitamente: {total}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[!] Ocorreu um erro SQL: {e}")

if __name__ == "__main__":
    main()
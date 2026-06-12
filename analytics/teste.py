import boto3
import redshift_connector
import subprocess
import time

# Configurações de acesso ao Redshift
DB_NAME = 'dev'
MASTER_USER = 'awsuser'
MASTER_PASS = 'SenhaForte2026!'

def get_terraform_output(key):
    """Busca o nome do bucket e outras infos direto do Terraform"""
    result = subprocess.run(
        ["terraform", "output", "-raw", key],
        capture_output=True, text=True, cwd="terraform"
    )
    return result.stdout.strip()

def main():
    # 1. Recuperar o nome do bucket do Datalake que o Firehose usa
    try:
        bucket_name = get_terraform_output("datalake_bucket_name")
        print(f"[*] Bucket Datalake detectado: {bucket_name}")
    except:
        print("[!] Erro: Output 'datalake_bucket_name' não encontrado no Terraform.")
        return

    # 2. Buscar o Endpoint do Redshift que foi criado
    client = boto3.client('redshift', region_name='us-east-1')
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    lab_role_arn = f"arn:aws:iam::{account_id}:role/LabRole"

    clusters = client.describe_clusters()
    # Pega o primeiro cluster que estiver disponível e comece com 'dijkfood'
    try:
        meu_cluster = [c for c in clusters['Clusters'] if c['ClusterIdentifier'].startswith('dijkfood')][0]
        endpoint = meu_cluster['Endpoint']['Address']
        port = meu_cluster['Endpoint']['Port']
    except IndexError:
        print("[!] Erro: Nenhum cluster Redshift ativo encontrado.")
        return

    print(f"[*] Conectando ao Redshift em: {endpoint}")

    try:
        conn = redshift_connector.connect(
            host=endpoint, database=DB_NAME, user=MASTER_USER, password=MASTER_PASS, port=port
        )
        cursor = conn.cursor()

        # 3. Executar o COPY para trazer os dados do S3 para o Redshift
        print("[*] Sincronizando dados do S3 (Datalake) para o Redshift...")
        cursor.execute(f"""
            COPY pedidos 
            FROM 's3://{bucket_name}/pedidos/' 
            IAM_ROLE '{lab_role_arn}' 
            FORMAT AS JSON 'auto' 
            REGION 'us-east-1';
        """)
        conn.commit()

        # 4. Verificar se há dados
        cursor.execute("SELECT COUNT(*) FROM pedidos;")
        qtd = cursor.fetchone()[0]
        
        print("\n" + "="*40)
        print(f"📊 RESULTADO DO TESTE ANALÍTICO")
        print(f"Total de pedidos processados no Redshift: {qtd}")
        print("="*40)
        
        if qtd > 0:
            print("✅ SUCESSO: O pipeline Kinesis -> Firehose -> S3 -> Redshift está funcionando!")
        else:
            print("⏳ AGUARDANDO: O buffer do Firehose (60s) ainda não descarregou arquivos no S3.")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[!] Erro na conexão ou consulta: {e}")

if __name__ == "__main__":
    main()
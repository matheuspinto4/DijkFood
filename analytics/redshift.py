import redshift_connector
import subprocess

def get_terraform_output(key):
    result = subprocess.run(["terraform", "output", "-raw", key], capture_output=True, text=True, cwd="terraform")
    return result.stdout.strip()

def main():
    # 1. Pega os dados já criados pelo Terraform
    endpoint = get_terraform_output("redshift_endpoint")
    NOME_DO_BUCKET_S3 = get_terraform_output("datalake_bucket_name")
    
    # IAM Role vinda do Terraform ou Hardcoded para o Lab
    lab_role_arn = "arn:aws:iam::340621435102:role/LabRole" 

    print(f"[*] Conectando ao cluster gerenciado pelo Terraform: {endpoint}")

    conn = redshift_connector.connect(
        host=endpoint.split(':')[0], # O endpoint vem com porta no Redshift
        database='dev', user='awsuser', password='SenhaForte2026!', port=5439
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # 2. Configura apenas a parte lógica
    cursor.execute(f"""
        CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_schema
        FROM DATA CATALOG DATABASE 'dijkfood_db'
        IAM_ROLE '{lab_role_arn}' CREATE EXTERNAL DATABASE IF NOT EXISTS;
    """)
    
    cursor.execute("DROP TABLE IF EXISTS spectrum_schema.pedidos_stream;")
    
    # 3. Criação da tabela mantendo as novas colunas analíticas, mas lendo JSON
    cursor.execute(f"""
        CREATE EXTERNAL TABLE spectrum_schema.pedidos_stream (
            id_pedido INT, 
            id_cliente INT, 
            id_restaurante INT, 
            nome_restaurante VARCHAR(255),
            id_entregador INT, 
            status VARCHAR(50),
            timestamp VARCHAR(50),
            latitude_cliente DOUBLE PRECISION,
            longitude_cliente DOUBLE PRECISION,
            latitude_restaurante DOUBLE PRECISION,
            longitude_restaurante DOUBLE PRECISION
        )
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
        STORED AS TEXTFILE LOCATION 's3://{NOME_DO_BUCKET_S3}/pedidos/';
    """)
    
    print("✅ Schema e Tabela Externa configurados com sucesso!")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
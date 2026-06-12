import boto3
import redshift_connector
import time
import random
import subprocess

# --- Configurações de Infraestrutura do Cluster ---
sufixo_aux = random.randint(1000, 9999)
CLUSTER_IDENTIFIER = f'dijkfood-analytics-cluster-{sufixo_aux}'
DB_NAME = 'dev'
MASTER_USER = 'awsuser'
MASTER_PASS = 'SenhaForte2026!'
NODE_TYPE = 'ra3.large'  

def get_terraform_output(key):
    """Lê dinamicamente as saídas do Terraform na pasta correta"""
    result = subprocess.run(
        ["terraform", "output", "-raw", key],
        capture_output=True, text=True, cwd="terraform"
    )
    if result.returncode != 0:
        raise RuntimeError(f"Erro ao ler terraform output '{key}':\n{result.stderr}")
    return result.stdout.strip()

def get_lab_role_arn():
    """Identifica a conta AWS atual e monta o ARN da LabRole do ambiente de laboratório"""
    sts = boto3.client('sts')
    account_id = sts.get_caller_identity()['Account']
    return f"arn:aws:iam::{account_id}:role/LabRole"

def setup_security_group():
    """Cria ou recupera o Security Group liberando tráfego de entrada na porta 5432"""
    ec2 = boto3.client('ec2', region_name='us-east-1')
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
    vpc_id = vpcs["Vpcs"][0]["VpcId"]
    sg_name = f"redshift-dijkfood-sg"

    try:
        sg = ec2.create_security_group(
            GroupName=sg_name,
            Description="Permitir acesso ao Redshift na porta 5432 para Analytics",
            VpcId=vpc_id
        )
        sg_id = sg['GroupId']
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{'IpProtocol': 'tcp', 'FromPort': 5432, 'ToPort': 5432, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}] }]
        )
        print(f"[*] Security Group criado: {sg_id}")
        return sg_id
        
    except Exception as e:
        if "InvalidGroup.Duplicate" in str(e):
            sgs = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [sg_name]}])
            return sgs['SecurityGroups'][0]['GroupId']
        raise e

def main():
    redshift = boto3.client('redshift', region_name='us-east-1')
    lab_role_arn = get_lab_role_arn()
    print(f"[*] Utilizando LabRole: {lab_role_arn}")

    sg_id = setup_security_group()

    # 1. SOLICITAR PROVISIONAMENTO DO CLUSTER
    print(f"[*] Iniciando a criação do cluster '{CLUSTER_IDENTIFIER}'...")
    try:
        redshift.create_cluster(
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            NodeType=NODE_TYPE,
            ClusterType='multi-node',
            NumberOfNodes=2,
            DBName=DB_NAME,
            MasterUsername=MASTER_USER,
            MasterUserPassword=MASTER_PASS,
            IamRoles=[lab_role_arn],
            PubliclyAccessible=True,
            VpcSecurityGroupIds=[sg_id],
            Port=5432  
        )
    except Exception as e:
        print(f"[!] Erro ao criar cluster: {e}")
        return

    # 2. AGUARDAR INFRAESTRUTURA FICAR DISPONÍVEL
    print("[*] Aguardando o cluster ficar disponível (Isso pode levar alguns minutos)...")
    waiter = redshift.get_waiter('cluster_available')
    waiter.wait(ClusterIdentifier=CLUSTER_IDENTIFIER)
    
    cluster_info = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
    endpoint = cluster_info['Endpoint']['Address']
    port = cluster_info['Endpoint']['Port']
    print(f"[*] Cluster pronto! Endpoint: {endpoint}:{port}")

    # 3. LÓGICA DE RETRY PARA PROPAGAÇÃO DE REDE/DNS
    print("[*] Aguardando propagação da rede (Tentando conectar ao banco)...")
    conn = None
    tentativas = 0
    max_tentativas = 5
    
    while tentativas < max_tentativas:
        try:
            conn = redshift_connector.connect(
                host=endpoint, database=DB_NAME, user=MASTER_USER, password=MASTER_PASS, port=port,
                timeout=15
            )
            print("[*] Conexão estabelecida com sucesso!")
            break
        except Exception as e:
            tentativas += 1
            print(f"    -> Tentativa {tentativas}/{max_tentativas} falhou. Aguardando 20 segundos...")
            time.sleep(20)
            
    if not conn:
        print("[!] Erro: Não foi possível alcançar o cluster. Verifique se a rede atual bloqueia a porta 5432.")
        return

    try:
        cursor = conn.cursor()

        # 4. PIPELINE ELT - ETAPA 1: EXTRACT & LOAD (CARGA BRUTA VIA NOSHRED)
        print("[*] [ELT - LOAD] Criando Staging Table (pedidos_raw)...")
        cursor.execute("DROP TABLE IF EXISTS pedidos;")
        cursor.execute("DROP TABLE IF EXISTS pedidos_raw;")
        cursor.execute("CREATE TABLE pedidos_raw (dado SUPER);")
        conn.commit()

        NOME_DO_BUCKET_S3 = get_terraform_output("datalake_bucket_name")
        print(f"[*] [ELT - LOAD] Sugando JSONs brutos do S3 para a Staging Table...")
        
        t_inicio_copy = time.perf_counter()
        cursor.execute(f"""
            COPY pedidos_raw 
            FROM 's3://{NOME_DO_BUCKET_S3}/' 
            IAM_ROLE '{lab_role_arn}' 
            FORMAT AS JSON 'noshred' 
            REGION 'us-east-1'; 
        """) 
        conn.commit()
        t_fim_copy = time.perf_counter()
        print(f"    -> Ingestão bruta concluída em: {t_fim_copy - t_inicio_copy:.2f} segundos")

        # 5. PIPELINE ELT - ETAPA 2: TRANSFORM (ESTRUTURAÇÃO VIA SQL)
        print("[*] [ELT - TRANSFORM] Criando Tabela Analítica Final Otimizada (pedidos)...")
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
        conn.commit()

        print("[*] [ELT - TRANSFORM] Desempacotando, tipando e movendo os dados para a tabela final...")
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
        print("[*] Processo de Engenharia ELT finalizado com absoluto sucesso!")

        # 6. VALIDAÇÃO ANALÍTICA FINAL
        cursor.execute("SELECT COUNT(*) FROM pedidos;")
        total = cursor.fetchone()[0]
        print(f"\n✅ VALIDAÇÃO: Total de pedidos consolidados no Data Warehouse: {total}")

        print("[*] Executando query de inteligência de negócios (Top 5 Restaurantes)...")
        cursor.execute("""
            SELECT id_restaurante, COUNT(id_pedido) as total_pedidos
            FROM pedidos 
            GROUP BY id_restaurante 
            ORDER BY total_pedidos DESC 
            LIMIT 5;
        """)
        resultados = cursor.fetchall()
        
        print("\n--- TOP 5 RESTAURANTES POR VOLUME ---")
        for linha in resultados:
            print(f"Restaurante ID: {linha[0]:<5} | Total de Pedidos: {linha[1]}")
        print("-------------------------------------\n")

        cursor.close()
        conn.close()
        print(f"[*] Setup Analítico Concluído. Guarde este endpoint para o Streamlit: {endpoint}")
        
    except Exception as e:
        print(f"[!] Erro crítico na execução das queries SQL: {e}")

if __name__ == "__main__":
    main()
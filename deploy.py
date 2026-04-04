"""
deploy_dijkfood.py — Full Architecture Deployment (RDS, DDB, S3, EC2)
Criado para o projeto DijkFood

Lifecycle:
  1. allocate   — Cria RDS, DynamoDB, S3 Bucket e Instância EC2
  2. populate   — Injeta dados (Faker) no RDS/DDB e gera/faz upload do Grafo (OSMnx) no S3
  3. destroy    — Esvazia/Apaga S3, deleta RDS, DDB e termina a instância EC2

Usage:
  python deploy_dijkfood.py                           # Cria, popula e APAGA em seguida
  python deploy_dijkfood.py --step allocate           # Apenas cria a infraestrutura
  python deploy_dijkfood.py --step populate           # Apenas conecta e popula os dados
  python deploy_dijkfood.py --step destroy            # Apaga toda a infraestrutura
"""

import argparse
import boto3
import psycopg2
import psycopg2.extras
import random
import time
import uuid
import csv
import os
from decimal import Decimal
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

from faker import Faker
from faker.providers import BaseProvider

# ── Custom Faker Provider (RDS) ────────────────────────────────────────────────
class RestauranteProvider(BaseProvider):
    tipos_culinaria = [
        'Pizzaria', 'Churrascaria', 'Comida Japonesa', 'Comida Mineira', 
        'Comida Baiana', 'Lanchonete', 'Hamburgueria', 'Cantina Italiana', 
        'Comida Vegana', 'Bistrô Francês', 'Frutos do Mar'
    ]
    prefixos = ['Restaurante', 'Cantina', 'Pizzaria', 'Bar e Petiscaria', 'Recanto', 'Espaço']
    sufixos = ['Gourmet', 'da Família', 'Tradicional', 'Express', 'Saboroso', 'Grill']

    def tipo_restaurante(self):
        return self.random_element(self.tipos_culinaria)

    def nome_restaurante(self):
        formato = random.choice(['nome_pessoa', 'prefixo_sufixo', 'sobrenome'])
        if formato == 'nome_pessoa':
            artigo = self.random_element(['do', 'da'])
            nome = self.generator.first_name()
            estabelecimento = self.random_element(['Bar', 'Restaurante', 'Cantina', 'Lanchonete'])
            return f"{estabelecimento} {artigo} {nome}"
        elif formato == 'prefixo_sufixo':
            return f"{self.random_element(self.prefixos)} {self.random_element(self.sufixos)}"
        else:
            estabelecimento = self.random_element(self.prefixos)
            sobrenome = self.generator.last_name()
            return f"{estabelecimento} {sobrenome}"

def get_lat_lon(): 
    return random.uniform(-23.7, -23.4), random.uniform(-46.8, -46.3)


# ── Configuration ──────────────────────────────────────────────────────────────
REGION         = "us-east-1"
ACCOUNT_ID     = boto3.client('sts').get_caller_identity().get('Account')

# Configurações RDS
DB_INSTANCE_ID = "dijkfood-primary"
DB_NAME        = "dijkfooddb"
DB_ADMIN_USER  = "dijk_admin"
DB_PASSWORD    = "DijkFood2026!Cloud"  
DB_PORT        = 5432
INSTANCE_CLASS = "db.t3.micro"
PG_VERSION     = "16"
SG_NAME        = "dijkfood-sg"
PG_GROUP_NAME  = "dijkfood-pg16"   

# Dados
N_CLIENTES     = 1000
N_RESTAURANTES = 50
N_ENTREGADORES = 3000
N_PEDIDOS      = 50000
N_DDB_ITEMS    = 50000

# Configurações DynamoDB
DDB_TABLE_EVENTOS    = "dijkfood-historico-eventos"
DDB_TABLE_TELEMETRIA = "dijkfood-telemetria-entregadores"

# Configurações S3 & Grafo
S3_BUCKET_NAME = f"dijkfood-grafo-sp-{ACCOUNT_ID}"
PLACE_NAME     = "São Paulo, Brazil"
NETWORK_TYPE   = "drive"

# Configurações EC2
EC2_INSTANCE_TYPE = "t3.micro"
KEY_PAIR_NAME     = "vockey" # Chave padrão do AWS Academy


# ─────────────────────────────────────────────────────────────────────────────
# 1. ALLOCATION (RDS + DDB + S3 + EC2)
# ─────────────────────────────────────────────────────────────────────────────

def get_clients():
    session = boto3.Session(region_name=REGION)
    return session.client("rds"), session.client("ec2"), session.resource("dynamodb"), session.client("s3")

# --- RDS & Security ---
def create_parameter_group(rds):
    print(f"[PG]  Creating parameter group '{PG_GROUP_NAME}' ...")
    try:
        rds.create_db_parameter_group(
            DBParameterGroupName=PG_GROUP_NAME,
            DBParameterGroupFamily=f"postgres{PG_VERSION}",
            Description="DijkFood parameter group - postgres 16",
        )
        rds.modify_db_parameter_group(
            DBParameterGroupName=PG_GROUP_NAME,
            Parameters=[{"ParameterName": "work_mem", "ParameterValue": "4096", "ApplyMethod": "immediate"}],
        )
        print("[PG]  Created  (work_mem = 4 MB)")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "DBParameterGroupAlreadyExists":
            print("[PG]  Already exists, reusing.")
        else: raise
    return PG_GROUP_NAME

def create_security_group(ec2):
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
    if not vpcs["Vpcs"]: raise RuntimeError("No default VPC found.")
    vpc_id = vpcs["Vpcs"][0]["VpcId"]

    try:
        sg = ec2.create_security_group(GroupName=SG_NAME, Description="DijkFood General SG", VpcId=vpc_id)
        sg_id = sg["GroupId"]
        # Libera Postgres e SSH
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {"IpProtocol": "tcp", "FromPort": DB_PORT, "ToPort": DB_PORT, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
                {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}
            ],
        )
        print(f"[SG]  Created {sg_id}  (VPC: {vpc_id})")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "InvalidGroup.Duplicate":
            existing = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [SG_NAME]}])
            sg_id = existing["SecurityGroups"][0]["GroupId"]
            print(f"[SG]  Already exists: {sg_id}")
        else: raise
    return sg_id

def allocate_rds(rds, sg_id, pg_group):
    print(f"[RDS] Creating '{DB_INSTANCE_ID}' ({INSTANCE_CLASS} / postgres{PG_VERSION}) ...")
    try:
        rds.create_db_instance(
            DBInstanceIdentifier=DB_INSTANCE_ID, DBInstanceClass=INSTANCE_CLASS, Engine="postgres",
            EngineVersion=PG_VERSION, MasterUsername=DB_ADMIN_USER, MasterUserPassword=DB_PASSWORD,
            DBName=DB_NAME, AllocatedStorage=20, StorageType="gp2", VpcSecurityGroupIds=[sg_id],
            DBParameterGroupName=pg_group, PubliclyAccessible=True, BackupRetentionPeriod=1, MultiAZ=False
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "DBInstanceAlreadyExists":
            print("[RDS] Already exists, skipping creation.")
        else: raise

# --- DynamoDB ---
def allocate_dynamodb(ddb):
    for tb_name, pk_name in [(DDB_TABLE_EVENTOS, "id_pedido"), (DDB_TABLE_TELEMETRIA, "id_entregador")]:
        print(f"[DDB] Creating table '{tb_name}' ...")
        try:
            table = ddb.create_table(
                TableName=tb_name,
                KeySchema=[{"AttributeName": pk_name, "KeyType": "HASH"}, {"AttributeName": "timestamp", "KeyType": "RANGE"}],
                AttributeDefinitions=[{"AttributeName": pk_name, "AttributeType": "S"}, {"AttributeName": "timestamp", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST"
            )
            table.wait_until_exists()
            print(f"[DDB] Table {tb_name} active.")
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ResourceInUseException":
                print(f"[DDB] Table {tb_name} already exists, reusing.")
            else: raise

# --- S3 ---
def allocate_s3(s3):
    print(f"[S3]  Creating bucket '{S3_BUCKET_NAME}' ...")
    try:
        s3.create_bucket(Bucket=S3_BUCKET_NAME)
        print(f"[S3]  Bucket created.")
    except ClientError as e:
        if e.response['Error']['Code'] in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
            print(f"[S3]  Bucket already exists, reusing.")
        else: raise

# --- EC2 ---
def get_latest_amazon_linux_ami():
    ssm = boto3.client("ssm", region_name=REGION)
    param = ssm.get_parameter(Name="/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2")
    return param["Parameter"]["Value"]

def allocate_ec2(ec2, sg_id):
    ami_id = get_latest_amazon_linux_ami()
    print(f"[EC2] Creating '{EC2_INSTANCE_TYPE}' | AMI {ami_id} ...")
    
    try:
        response = ec2.run_instances(
            ImageId=ami_id, InstanceType=EC2_INSTANCE_TYPE, KeyName=KEY_PAIR_NAME,
            SecurityGroupIds=[sg_id], MinCount=1, MaxCount=1,
            TagSpecifications=[{"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": "DijkFood-Worker"}]}]
        )
        instance_id = response["Instances"][0]["InstanceId"]
        print(f"[EC2] Instance ID: {instance_id} starting...")
    except ClientError as e:
        print(f"[EC2] Warning: Could not create EC2. {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 2. POPULATE (RDS + DDB + Grafos no S3)
# ─────────────────────────────────────────────────────────────────────────────

def connect(endpoint, user=DB_ADMIN_USER, password=DB_PASSWORD, retries=6, delay=10):
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(host=endpoint, port=DB_PORT, dbname=DB_NAME, user=user, password=password, connect_timeout=10)
            print(f"[DB]  Connected  host={endpoint}  user={user}")
            return conn
        except psycopg2.OperationalError as exc:
            if attempt == retries: raise
            print(f"[DB]  Attempt {attempt}/{retries}: {exc}. Retrying in {delay}s ...")
            time.sleep(delay)

def populate_rds(conn):
    print("\n--- Populando RDS (PostgreSQL) ---")
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clientes (id_cliente SERIAL PRIMARY KEY, nome VARCHAR(80), email VARCHAR(80), telefone VARCHAR(20), latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);
            CREATE TABLE IF NOT EXISTS restaurantes (id_restaurante SERIAL PRIMARY KEY, nome VARCHAR(80), tipo_cozinha VARCHAR(40), latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);
            CREATE TABLE IF NOT EXISTS entregadores (id_entregador SERIAL PRIMARY KEY, nome VARCHAR(80), tipo_veiculo VARCHAR(30), latitude_inicial DOUBLE PRECISION, longitude_inicial DOUBLE PRECISION, status_ocupado BOOLEAN DEFAULT FALSE);
            CREATE TABLE IF NOT EXISTS pedidos (id_pedido SERIAL PRIMARY KEY, id_cliente INT REFERENCES clientes(id_cliente), id_restaurante INT REFERENCES restaurantes(id_restaurante), id_entregador INT REFERENCES entregadores(id_entregador), valor NUMERIC(8,2), data_criacao TIMESTAMP);
            CREATE INDEX IF NOT EXISTS idx_entregador ON entregadores(status_ocupado) WHERE status_ocupado = FALSE;
        """)
        
        fake = Faker(['pt-BR'])
        fake.add_provider(RestauranteProvider)

        print(f"Gerando {N_CLIENTES} clientes, {N_RESTAURANTES} restaurantes, {N_ENTREGADORES} entregadores...")
        clientes = [(fake.name(), fake.email(), fake.phone_number(), *get_lat_lon()) for _ in range(N_CLIENTES)]
        restaurantes = [(fake.nome_restaurante(), fake.tipo_restaurante(), *get_lat_lon()) for _ in range(N_RESTAURANTES)]
        entregadores = [(fake.name(), random.choice(["moto", "carro", "biscicleta"]), *get_lat_lon(), False) for _ in range(N_ENTREGADORES)]

        psycopg2.extras.execute_values(cur, "INSERT INTO clientes (nome, email, telefone, latitude, longitude) VALUES %s", clientes, page_size=500)
        psycopg2.extras.execute_values(cur, "INSERT INTO restaurantes (nome, tipo_cozinha, latitude, longitude) VALUES %s", restaurantes, page_size=100)
        psycopg2.extras.execute_values(cur, "INSERT INTO entregadores (nome, tipo_veiculo, latitude_inicial, longitude_inicial, status_ocupado) VALUES %s", entregadores, page_size=2000)

        cur.execute("SELECT id_cliente FROM clientes"); clientes_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id_restaurante FROM restaurantes"); rest_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id_entregador FROM entregadores"); ent_ids = [r[0] for r in cur.fetchall()]

        print(f"Gerando {N_PEDIDOS} pedidos históricos...")
        batch, base_date = [], datetime(2023, 1, 1)
        for i in range(N_PEDIDOS):
            data_pedido = base_date + timedelta(days=random.randint(0, 365), hours=random.randint(0,23))
            batch.append((random.choice(clientes_ids), random.choice(rest_ids), random.choice(ent_ids), round(random.uniform(15.0, 250.0), 2), data_pedido.isoformat()))
            if len(batch) == 5000:
                psycopg2.extras.execute_values(cur, "INSERT INTO pedidos (id_cliente, id_restaurante, id_entregador, valor, data_criacao) VALUES %s", batch, page_size=2000)
                batch = []; print(f"  {i + 1:,}/{N_PEDIDOS:,} pedidos inseridos ...")
        if batch: psycopg2.extras.execute_values(cur, "INSERT INTO pedidos (id_cliente, id_restaurante, id_entregador, valor, data_criacao) VALUES %s", batch, page_size=2000)
    conn.commit()

def populate_dynamodb(ddb):
    print(f"\n--- Populando DynamoDB (NoSQL) ---")
    tb_eventos = ddb.Table(DDB_TABLE_EVENTOS)
    tb_telemetria = ddb.Table(DDB_TABLE_TELEMETRIA)
    base_ts = datetime.now() - timedelta(days=30)
    
    print(f"[DDB] Injetando {N_DDB_ITEMS:,} logs de eventos...")
    with tb_eventos.batch_writer() as batch:
        for i in range(N_DDB_ITEMS):
            batch.put_item(Item={
                "id_pedido": f"PEDIDO#{random.randint(1, N_PEDIDOS)}", "timestamp": (base_ts + timedelta(minutes=i)).isoformat(),
                "status": random.choice(["criado", "preparando", "saiu_para_entrega", "entregue"]), "event_id": uuid.uuid4().hex[:8]
            })

    print(f"[DDB] Injetando {N_DDB_ITEMS:,} pontos de telemetria...")
    with tb_telemetria.batch_writer() as batch:
        for i in range(N_DDB_ITEMS):
            lat, lon = get_lat_lon()
            batch.put_item(Item={
                "id_entregador": f"ENTREGADOR#{random.randint(1, N_ENTREGADORES)}", "timestamp": (base_ts + timedelta(seconds=i*30)).isoformat(),
                "latitude": Decimal(str(round(lat, 6))), "longitude": Decimal(str(round(lon, 6))), "bateria": Decimal(str(random.randint(10, 100)))
            })

def populate_graph_s3(s3):
    print(f"\n--- Baixando Grafo OSMnx e enviando para S3 ---")
    import osmnx as ox
    
    edges_file = "graph_edges.csv"
    nodes_file = "graph_nodes.csv"
    
    if not os.path.exists(edges_file):
        print(f"[OSMnx] Baixando a rede '{NETWORK_TYPE}' para '{PLACE_NAME}' (Pode levar alguns minutos)...")
        graph = ox.graph_from_place(PLACE_NAME, network_type=NETWORK_TYPE)
        
        seen_edges = {}
        for u, v, data in graph.edges(data=True):
            w = float(data.get("length", 1.0))
            key = (u, v)
            if key not in seen_edges or w < seen_edges[key]:
                seen_edges[key] = w

        with open(edges_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["from_node", "to_node", "weight"])
            for (u, v), w in sorted(seen_edges.items()):
                writer.writerow([u, v, f"{w:.4f}"])

        with open(nodes_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["node_id", "lat", "lon"])
            for n, data in graph.nodes(data=True):
                writer.writerow([n, data['y'], data['x']])
        print(f"[OSMnx] Arquivos CSV gerados localmente.")
    else:
        print("[OSMnx] Arquivos locais encontrados. Pulando download.")

    print(f"[S3] Fazendo upload para s3://{S3_BUCKET_NAME}/")
    s3.upload_file(edges_file, S3_BUCKET_NAME, edges_file)
    s3.upload_file(nodes_file, S3_BUCKET_NAME, nodes_file)
    print("[S3] Upload concluído!")


# ─────────────────────────────────────────────────────────────────────────────
# 3. TEARDOWN (Esvaziar S3, Apagar EC2, RDS e DDB)
# ─────────────────────────────────────────────────────────────────────────────

def destroy(rds, ec2, ddb, s3):
    print("\n── Teardown " + "─" * 55)
    
    # 1. EC2
    print(f"[EC2] Terminating instances with name 'DijkFood-Worker' ...")
    instances = ec2.describe_instances(Filters=[{"Name": "tag:Name", "Values": ["DijkFood-Worker"]}])
    for res in instances.get("Reservations", []):
        for inst in res.get("Instances", []):
            if inst["State"]["Name"] != "terminated":
                ec2.terminate_instances(InstanceIds=[inst["InstanceId"]])
                print(f"      Terminated {inst['InstanceId']}")

    # 2. S3 (Esvaziar e deletar)
    print(f"[S3]  Emptying and deleting bucket '{S3_BUCKET_NAME}' ...")
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
        if 'Contents' in response:
            objects = [{'Key': obj['Key']} for obj in response['Contents']]
            s3.delete_objects(Bucket=S3_BUCKET_NAME, Delete={'Objects': objects})
        s3.delete_bucket(Bucket=S3_BUCKET_NAME)
        print(f"[S3]  Bucket deleted.")
    except ClientError as e:
        print(f"[S3]  Skipping: {e.response['Error']['Code']}")

    # 3. DynamoDB
    for tb_name in [DDB_TABLE_EVENTOS, DDB_TABLE_TELEMETRIA]:
        print(f"[DDB] Deleting table '{tb_name}' ...")
        try:
            ddb.Table(tb_name).delete()
            print(f"[DDB] Table {tb_name} deleted.")
        except ClientError: pass

    # 4. RDS & Security Groups
    print(f"[RDS] Deleting primary '{DB_INSTANCE_ID}' ...")
    try:
        rds.delete_db_instance(DBInstanceIdentifier=DB_INSTANCE_ID, SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
        rds.get_waiter("db_instance_deleted").wait(DBInstanceIdentifier=DB_INSTANCE_ID, WaiterConfig={"Delay": 30, "MaxAttempts": 40})
        print(f"[RDS] Primary deleted.")
    except ClientError: pass

    try:
        rds.delete_db_parameter_group(DBParameterGroupName=PG_GROUP_NAME)
        print(f"[PG]  '{PG_GROUP_NAME}' deleted.")
    except ClientError: pass

    try:
        sgs = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [SG_NAME]}])
        if sgs["SecurityGroups"]:
            ec2.delete_security_group(GroupId=sgs["SecurityGroups"][0]["GroupId"])
            print(f"[SG]  {SG_NAME} deleted.")
    except ClientError: pass


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="DijkFood Full Architecture Deployment")
    parser.add_argument("--step", choices=["all", "allocate", "populate", "destroy"], default="all")
    args = parser.parse_args()

    rds, ec2, ddb, s3 = get_clients()

    if args.step in ("all", "allocate"):
        pg_group = create_parameter_group(rds)
        sg_id    = create_security_group(ec2)
        allocate_ec2(ec2, sg_id)
        allocate_dynamodb(ddb)
        allocate_s3(s3)
        allocate_rds(rds, sg_id, pg_group) 

    if args.step in ("all", "populate"):
        # Aguarda RDS ficar disponível antes de pedir o Endpoint
        try:
            print(f"[RDS] Aguardando o banco ficar 'Available' para recuperar o Endpoint (pode levar alguns minutos)...")
            rds.get_waiter("db_instance_available").wait(
                DBInstanceIdentifier=DB_INSTANCE_ID, WaiterConfig={"Delay": 30, "MaxAttempts": 40}
            )
            
            endpoint = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)["DBInstances"][0]["Endpoint"]["Address"]
            conn = connect(endpoint)
            if conn:
                populate_rds(conn)
                conn.close()
        except Exception as e:
            print(f"\nNão foi possível popular RDS: {e}")
        
        populate_dynamodb(ddb)
        populate_graph_s3(s3)

    if args.step in ("all", "destroy"):
        destroy(rds, ec2, ddb, s3)

if __name__ == "__main__":
    main()
"""
deploy_dijkfood.py — Amazon RDS & DynamoDB Deployment & Populate
Criado para o projeto DijkFood

Lifecycle:
  1. allocate   — Cria RDS (Postgres) + Tabelas DynamoDB
  2. populate   — Injeta dados relacionais (Faker) no RDS e logs/eventos no DynamoDB
  3. destroy    — Apaga a instância RDS e as tabelas DynamoDB

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

# Dados RDS
N_CLIENTES     = 1000
N_RESTAURANTES = 50
N_ENTREGADORES = 3000
N_PEDIDOS      = 50000

# Configurações DynamoDB
DDB_TABLE_EVENTOS    = "dijkfood-historico-eventos"
DDB_TABLE_TELEMETRIA = "dijkfood-telemetria-entregadores"
N_DDB_ITEMS          = 50_000


# ─────────────────────────────────────────────────────────────────────────────
# 1. ALLOCATION (RDS + DynamoDB)
# ─────────────────────────────────────────────────────────────────────────────

def get_clients():
    session = boto3.Session(region_name=REGION)
    return session.client("rds"), session.client("ec2"), session.resource("dynamodb")

# --- Funções de Alocação RDS ---
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
        else:
            raise
    return PG_GROUP_NAME

def create_security_group(ec2):
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
    if not vpcs["Vpcs"]:
        raise RuntimeError("No default VPC found.")
    vpc_id = vpcs["Vpcs"][0]["VpcId"]

    try:
        sg = ec2.create_security_group(GroupName=SG_NAME, Description="DijkFood RDS SG", VpcId=vpc_id)
        sg_id = sg["GroupId"]
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{"IpProtocol": "tcp", "FromPort": DB_PORT, "ToPort": DB_PORT, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}],
        )
        print(f"[SG]  Created {sg_id}  (VPC: {vpc_id})")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "InvalidGroup.Duplicate":
            existing = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [SG_NAME]}])
            sg_id = existing["SecurityGroups"][0]["GroupId"]
            print(f"[SG]  Already exists: {sg_id}")
        else:
            raise
    return sg_id

def allocate_rds(rds, sg_id, pg_group):
    print(f"[RDS] Creating '{DB_INSTANCE_ID}' ({INSTANCE_CLASS} / postgres{PG_VERSION}) ...")
    try:
        rds.create_db_instance(
            DBInstanceIdentifier=DB_INSTANCE_ID,
            DBInstanceClass=INSTANCE_CLASS,
            Engine="postgres",
            EngineVersion=PG_VERSION,
            MasterUsername=DB_ADMIN_USER,
            MasterUserPassword=DB_PASSWORD,
            DBName=DB_NAME,
            AllocatedStorage=20,
            StorageType="gp2",
            VpcSecurityGroupIds=[sg_id],
            DBParameterGroupName=pg_group,
            PubliclyAccessible=True,
            BackupRetentionPeriod=1,
            MultiAZ=False,
            AutoMinorVersionUpgrade=False,
            Tags=[{"Key": "purpose", "Value": "dijkfood-demo"}],
        )
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "DBInstanceAlreadyExists":
            print("[RDS] Already exists, skipping creation.")
        else:
            raise

    print("[RDS] Waiting for 'available' (typically 5–8 min) ...")
    waiter = rds.get_waiter("db_instance_available")
    waiter.wait(DBInstanceIdentifier=DB_INSTANCE_ID, WaiterConfig={"Delay": 30, "MaxAttempts": 40})

    info = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)
    endpoint = info["DBInstances"][0]["Endpoint"]["Address"]
    print(f"[RDS] Ready  endpoint={endpoint}")
    return endpoint

# --- Funções de Alocação DynamoDB ---
def create_ddb_table(ddb, table_name, pk_name):
    print(f"[DDB] Creating table '{table_name}' ...")
    try:
        table = ddb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": pk_name, "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": pk_name, "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            Tags=[{"Key": "purpose", "Value": "dijkfood-demo"}]
        )
        table.wait_until_exists()
        print(f"[DDB] Table {table_name} active.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceInUseException":
            print(f"[DDB] Table {table_name} already exists, reusing.")
        else:
            raise

def allocate_dynamodb(ddb):
    create_ddb_table(ddb, DDB_TABLE_EVENTOS, "id_pedido")
    create_ddb_table(ddb, DDB_TABLE_TELEMETRIA, "id_entregador")


def get_primary_endpoint(rds):
    info = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)
    return info["DBInstances"][0]["Endpoint"]["Address"]

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
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 2. POPULATE (RDS + DynamoDB)
# ─────────────────────────────────────────────────────────────────────────────

# --- Populate RDS ---
DDL = """
CREATE TABLE IF NOT EXISTS clientes (
    id_cliente SERIAL PRIMARY KEY,
    nome VARCHAR(80) NOT NULL,
    email VARCHAR(80) NOT NULL,
    telefone VARCHAR(20),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS restaurantes (
    id_restaurante SERIAL PRIMARY KEY,
    nome VARCHAR(80) NOT NULL,
    tipo_cozinha VARCHAR(40) NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);
CREATE TABLE IF NOT EXISTS entregadores (
    id_entregador SERIAL PRIMARY KEY,
    nome VARCHAR(80) NOT NULL,
    tipo_veiculo VARCHAR(30) NOT NULL,
    latitude_inicial DOUBLE PRECISION,
    longitude_inicial DOUBLE PRECISION,
    status_ocupado BOOLEAN DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS pedidos (
    id_pedido SERIAL PRIMARY KEY,
    id_cliente INT NOT NULL REFERENCES clientes(id_cliente),
    id_restaurante INT NOT NULL REFERENCES restaurantes(id_restaurante),
    id_entregador INT NOT NULL REFERENCES entregadores(id_entregador),
    valor NUMERIC(8,2) NOT NULL,
    data_criacao TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_entregador ON entregadores(status_ocupado) WHERE status_ocupado = FALSE;
"""

def populate_rds(conn):
    print("\n--- Populando RDS (PostgreSQL) ---")
    with conn.cursor() as cur:
        cur.execute(DDL)
        fake = Faker(['pt-BR'])
        fake.add_provider(RestauranteProvider)

        print(f"Gerando dados fakes ({N_CLIENTES} clientes, {N_RESTAURANTES} restaurantes, {N_ENTREGADORES} entregadores)...")
        clientes = [(fake.name(), fake.email(), fake.phone_number(), *get_lat_lon()) for _ in range(N_CLIENTES)]
        restaurantes = [(fake.nome_restaurante(), fake.tipo_restaurante(), *get_lat_lon()) for _ in range(N_RESTAURANTES)]
        entregadores = [(fake.name(), random.choice(["moto", "carro", "caminhao", "biscicleta"]), *get_lat_lon(), False) for _ in range(N_ENTREGADORES)]

        psycopg2.extras.execute_values(cur, "INSERT INTO clientes (nome, email, telefone, latitude, longitude) VALUES %s", clientes, page_size=500)
        psycopg2.extras.execute_values(cur, "INSERT INTO restaurantes (nome, tipo_cozinha, latitude, longitude) VALUES %s", restaurantes, page_size=100)
        psycopg2.extras.execute_values(cur, "INSERT INTO entregadores (nome, tipo_veiculo, latitude_inicial, longitude_inicial, status_ocupado) VALUES %s", entregadores, page_size=2000)

        cur.execute("SELECT id_cliente FROM clientes")
        clientes_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id_restaurante FROM restaurantes")
        rest_ids = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT id_entregador FROM entregadores")
        ent_ids = [r[0] for r in cur.fetchall()]

        print(f"Gerando {N_PEDIDOS} pedidos históricos...")
        base_date = datetime(2023, 1, 1)
        batch = []
        for i in range(N_PEDIDOS):
            data_pedido = base_date + timedelta(days=random.randint(0, 365), hours=random.randint(0,23), minutes=random.randint(0,59))
            batch.append((
                random.choice(clientes_ids), random.choice(rest_ids), random.choice(ent_ids),
                round(random.uniform(15.0, 250.0), 2), data_pedido.isoformat()
            ))
            if len(batch) == 5000:
                psycopg2.extras.execute_values(cur, "INSERT INTO pedidos (id_cliente, id_restaurante, id_entregador, valor, data_criacao) VALUES %s", batch, page_size=2000)
                batch = []
                print(f"  {i + 1:,}/{N_PEDIDOS:,} pedidos inseridos ...")

        if batch:
            psycopg2.extras.execute_values(cur, "INSERT INTO pedidos (id_cliente, id_restaurante, id_entregador, valor, data_criacao) VALUES %s", batch, page_size=2000)
    conn.commit()
    print(f"[RDS] Done: DijkFood SQL populado com sucesso.")

# --- Populate DynamoDB ---
def populate_dynamodb(ddb):
    print(f"\n--- Populando DynamoDB (NoSQL) ---")
    tb_eventos = ddb.Table(DDB_TABLE_EVENTOS)
    tb_telemetria = ddb.Table(DDB_TABLE_TELEMETRIA)
    
    base_ts = datetime.now() - timedelta(days=30)
    status_list = ["criado", "preparando", "saiu_para_entrega", "entregue", "cancelado"]
    
    t0 = time.perf_counter()
    print(f"[DDB] Injetando {N_DDB_ITEMS:,} logs de eventos de pedidos...")
    with tb_eventos.batch_writer() as batch:
        for i in range(N_DDB_ITEMS):
            ts = base_ts + timedelta(minutes=i)
            batch.put_item(Item={
                "id_pedido": f"PEDIDO#{random.randint(1, N_PEDIDOS)}",
                "timestamp": ts.isoformat(),
                "status": random.choice(status_list),
                "detalhes": "Mudança de status registrada no app",
                "event_id": uuid.uuid4().hex[:8]
            })
    print(f"[DDB] Eventos injetados! ({(time.perf_counter() - t0):.1f}s)")

    t0 = time.perf_counter()
    print(f"[DDB] Injetando {N_DDB_ITEMS:,} pontos de telemetria de entregadores...")
    with tb_telemetria.batch_writer() as batch:
        for i in range(N_DDB_ITEMS):
            ts = base_ts + timedelta(seconds=i*30)
            lat, lon = get_lat_lon()
            batch.put_item(Item={
                "id_entregador": f"ENTREGADOR#{random.randint(1, N_ENTREGADORES)}",
                "timestamp": ts.isoformat(),
                "latitude": Decimal(str(round(lat, 6))),
                "longitude": Decimal(str(round(lon, 6))),
                "bateria": Decimal(str(random.randint(10, 100))),
                "velocidade_kmh": Decimal(str(random.randint(0, 60)))
            })
    print(f"[DDB] Telemetria injetada! ({(time.perf_counter() - t0):.1f}s)")


# ─────────────────────────────────────────────────────────────────────────────
# 3. TEARDOWN (RDS + DynamoDB)
# ─────────────────────────────────────────────────────────────────────────────

def destroy(rds, ec2, ddb):
    print("\n── Teardown " + "─" * 55)
    
    # Destrói DDB
    for tb_name in [DDB_TABLE_EVENTOS, DDB_TABLE_TELEMETRIA]:
        print(f"[DDB] Deleting table '{tb_name}' ...")
        try:
            table = ddb.Table(tb_name)
            table.delete()
            table.wait_until_not_exists()
            print(f"[DDB] Table {tb_name} deleted.")
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ResourceNotFoundException":
                print(f"[DDB] Table {tb_name} not found, skipping.")
            else:
                raise

    # Destrói RDS
    print(f"[RDS] Deleting primary '{DB_INSTANCE_ID}' ...")
    try:
        rds.delete_db_instance(DBInstanceIdentifier=DB_INSTANCE_ID, SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
        w = rds.get_waiter("db_instance_deleted")
        w.wait(DBInstanceIdentifier=DB_INSTANCE_ID, WaiterConfig={"Delay": 30, "MaxAttempts": 40})
        print(f"[RDS] Primary deleted.")
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("DBInstanceNotFound", "InvalidDBInstanceState"):
            print(f"[RDS] Primary not found or already deleted.")
        else: raise

    try:
        rds.delete_db_parameter_group(DBParameterGroupName=PG_GROUP_NAME)
        print(f"[PG]  '{PG_GROUP_NAME}' deleted.")
    except ClientError as exc:
        print(f"[PG]  Could not delete: {exc.response['Error']['Code']}")

    try:
        sgs = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [SG_NAME]}])
        if sgs["SecurityGroups"]:
            sg_id = sgs["SecurityGroups"][0]["GroupId"]
            ec2.delete_security_group(GroupId=sg_id)
            print(f"[SG]  {sg_id} deleted.")
    except ClientError as exc:
        print(f"[SG]  Could not delete: {exc.response['Error']['Code']}")

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="DijkFood RDS & DDB Deployment")
    parser.add_argument("--step", choices=["all", "allocate", "populate", "destroy"], default="all")
    args = parser.parse_args()

    rds, ec2, ddb = get_clients()

    if args.step in ("all", "allocate"):
        pg_group = create_parameter_group(rds)
        sg_id    = create_security_group(ec2)
        endpoint = allocate_rds(rds, sg_id, pg_group)
        allocate_dynamodb(ddb)
    else:
        try:
            endpoint = get_primary_endpoint(rds)
        except Exception:
            endpoint = None

    if args.step in ("all", "populate"):
        if endpoint:
            conn = connect(endpoint)
            if conn:
                populate_rds(conn)
                conn.close()
        populate_dynamodb(ddb)

    if args.step in ("all", "destroy"):
        destroy(rds, ec2, ddb)

if __name__ == "__main__":
    main()
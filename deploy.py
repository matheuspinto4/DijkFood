"""
deploy_dijkfood.py — Amazon RDS (PostgreSQL) Deployment & Populate
Criado para o projeto DijkFood

Lifecycle:
  1. allocate   — Cria parameter group, security group e a instância RDS
  2. populate   — Cria o schema e injeta os dados falsos (Faker)
  3. destroy    — Apaga a instância RDS, o parameter group e o security group

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
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

from faker import Faker
from faker.providers import BaseProvider

# ── Custom Faker Provider ──────────────────────────────────────────────────────
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
DB_INSTANCE_ID = "dijkfood-primary"
DB_NAME        = "dijkfooddb"
DB_ADMIN_USER  = "dijk_admin"
DB_PASSWORD    = "DijkFood2026!Cloud"  
DB_PORT        = 5432
INSTANCE_CLASS = "db.t3.micro"
PG_VERSION     = "16"
SG_NAME        = "dijkfood-sg"
PG_GROUP_NAME  = "dijkfood-pg16"   

N_CLIENTES     = 1000
N_RESTAURANTES = 50
N_ENTREGADORES = 3000
N_PEDIDOS      = 50000

# ─────────────────────────────────────────────────────────────────────────────
# 1. ALLOCATION
# ─────────────────────────────────────────────────────────────────────────────

def get_clients():
    session = boto3.Session(region_name=REGION)
    return session.client("rds"), session.client("ec2")

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
            Parameters=[{
                "ParameterName":  "work_mem",
                "ParameterValue": "4096",      # 4 MB
                "ApplyMethod":    "immediate",
            }],
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
        sg    = ec2.create_security_group(
            GroupName=SG_NAME,
            Description="DijkFood RDS - PostgreSQL inbound",
            VpcId=vpc_id,
        )
        sg_id = sg["GroupId"]
        ec2.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[{
                "IpProtocol": "tcp",
                "FromPort":   DB_PORT,
                "ToPort":     DB_PORT,
                "IpRanges":   [{"CidrIp": "0.0.0.0/0"}],
            }],
        )
        print(f"[SG]  Created {sg_id}  (VPC: {vpc_id})")
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "InvalidGroup.Duplicate":
            existing = ec2.describe_security_groups(
                Filters=[{"Name": "group-name", "Values": [SG_NAME]}]
            )
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
    waiter.wait(DBInstanceIdentifier=DB_INSTANCE_ID,
                WaiterConfig={"Delay": 30, "MaxAttempts": 40})

    info     = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)
    instance = info["DBInstances"][0]
    endpoint = instance["Endpoint"]["Address"]
    print(f"[RDS] Ready  endpoint={endpoint}  version={instance['EngineVersion']}")
    return endpoint

def get_primary_endpoint(rds):
    info = rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)
    return info["DBInstances"][0]["Endpoint"]["Address"]

def connect(endpoint, user=DB_ADMIN_USER, password=DB_PASSWORD, retries=6, delay=10, ssl=False):
    kwargs = dict(
        host=endpoint, port=DB_PORT, dbname=DB_NAME, user=user, password=password, connect_timeout=10
    )
    if ssl:
        kwargs["sslmode"] = "require"

    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**kwargs)
            print(f"[DB]  Connected  host={endpoint}  user={user}")
            return conn
        except psycopg2.OperationalError as exc:
            if attempt == retries:
                raise
            print(f"[DB]  Attempt {attempt}/{retries}: {exc}. Retrying in {delay}s ...")
            time.sleep(delay)
    return None

# ─────────────────────────────────────────────────────────────────────────────
# 2. POPULATE
# ─────────────────────────────────────────────────────────────────────────────

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

def populate(conn):
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

        # Fetch IDs to create relationships
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
                random.choice(clientes_ids),
                random.choice(rest_ids),
                random.choice(ent_ids),
                round(random.uniform(15.0, 250.0), 2),
                data_pedido.isoformat()
            ))
            if len(batch) == 5000:
                psycopg2.extras.execute_values(cur, "INSERT INTO pedidos (id_cliente, id_restaurante, id_entregador, valor, data_criacao) VALUES %s", batch, page_size=2000)
                batch = []
                print(f"  {i + 1:,}/{N_PEDIDOS:,} pedidos inseridos ...")

        if batch:
            psycopg2.extras.execute_values(cur, "INSERT INTO pedidos (id_cliente, id_restaurante, id_entregador, valor, data_criacao) VALUES %s", batch, page_size=2000)

    conn.commit()
    print(f"[DB]  Done: DijkFood populado com sucesso.")


# ─────────────────────────────────────────────────────────────────────────────
# 3. TEARDOWN
# ─────────────────────────────────────────────────────────────────────────────

def destroy(rds, ec2):
    print("\n── Teardown " + "─" * 55)
    
    print(f"[RDS] Deleting primary '{DB_INSTANCE_ID}' ...")
    try:
        rds.delete_db_instance(
            DBInstanceIdentifier=DB_INSTANCE_ID, SkipFinalSnapshot=True, DeleteAutomatedBackups=True,
        )
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
    parser = argparse.ArgumentParser(description="DijkFood RDS PostgreSQL Deployment")
    parser.add_argument("--step", choices=["all", "allocate", "populate", "destroy"], default="all")
    args = parser.parse_args()

    rds, ec2 = get_clients()

    if args.step in ("all", "allocate"):
        pg_group = create_parameter_group(rds)
        sg_id    = create_security_group(ec2)
        endpoint = allocate_rds(rds, sg_id, pg_group)
    else:
        # Se for rodar apenas o populate ou o destroy separadamente
        try:
            endpoint = get_primary_endpoint(rds)
        except Exception:
            endpoint = None

    if args.step in ("all", "populate") and endpoint:
        conn = connect(endpoint)
        if conn:
            populate(conn)
            conn.close()

    if args.step in ("all", "destroy"):
        destroy(rds, ec2)

if __name__ == "__main__":
    main()
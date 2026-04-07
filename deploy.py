"""
deploy.py — Full Architecture Deployment (RDS Multi-AZ, DDB, S3, ECS API, ECS Worker)
Criado para o projeto DijkFood

Lifecycle:
  1. allocate   — Cria toda a infraestrutura e as tabelas vazias no Banco de Dados
  2. populate   — Baixa o Grafo Viário (OSMnx) e envia para o S3
  3. destroy    — Apaga rigorosamente TODOS os recursos
  4. simulator  — Roda a simulação de tráfego
"""

import argparse
import boto3
import psycopg2
import time
import csv
import os
import osmnx as ox
from botocore.exceptions import ClientError
from simulator import main as simulator
import asyncio

# ── Configuration ──────────────────────────────────────────────────────────────
REGION         = "us-east-1"
try:
    ACCOUNT_ID = boto3.client('sts').get_caller_identity().get('Account')
    LAB_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/LabRole"
except Exception:
    ACCOUNT_ID = "000000000000"
    LAB_ROLE_ARN = ""

# Configurações RDS
DB_INSTANCE_ID = "dijkfood-primary"
DB_NAME        = "dijkfooddb"
DB_ADMIN_USER  = "dijk_admin"
DB_PASSWORD    = "DijkFood2026!Cloud"  
DB_PORT        = 5432
INSTANCE_CLASS = "db.t3.micro"
PG_VERSION     = "16"
PG_GROUP_NAME  = "dijkfood-pg16"   

# Configurações DynamoDB
DDB_TABLE_EVENTOS    = "dijkfood-historico-eventos"
DDB_TABLE_TELEMETRIA = "dijkfood-telemetria-entregadores"
DDB_TABLE_ALOCACAO_ENTREGADOR = "dijkfood-alocacao-entregador"

# Configurações S3 & Grafo
S3_BUCKET_NAME = f"dijkfood-grafo-sp-{ACCOUNT_ID}"
PLACE_NAME     = "São Paulo, Brazil"
NETWORK_TYPE   = "drive"

# Configurações ECS
ECS_CLUSTER_NAME = "DijkFoodCluster"
ALB_NAME         = "dijkfood-api-alb"
TG_NAME          = "dijkfood-api-tg"
API_PORT         = 80 


# ─────────────────────────────────────────────────────────────────────────────
# ARQUITETURA
# ─────────────────────────────────────────────────────────────────────────────
class Arquitetura:
    def __init__(self):
        self.rds, self.ec2, self.ddb, self.s3, self.ecs, self.elbv2, self.app_asg = self.get_clients()
        self.sgs = {}
        self.vpc_id = None
        self.pg_group = None
        self.rds_endpoint = None

    def get_clients(self):
        session = boto3.Session(region_name=REGION)
        return (
            session.client("rds"), 
            session.client("ec2"), 
            session.resource("dynamodb"), 
            session.client("s3"), 
            session.client("ecs"), 
            session.client("elbv2"),
            session.client("application-autoscaling")
        )
    
    def allocate(self):
        self.sgs, self.vpc_id = self.create_security_groups()
        self.pg_group = self.create_parameter_group()
        self.allocate_rds()
        self.allocate_dynamodb()
        self.allocate_s3()
        self.allocate_ecs_services()
        self.create_rds_schema()

    def create_security_groups(self):
        vpcs = self.ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        if not vpcs["Vpcs"]: raise RuntimeError("No default VPC found.")
        vpc_id = vpcs["Vpcs"][0]["VpcId"]
    
        print("[SG]  Configurando Security Groups...")
        sgs = {}
        
        def get_or_create_sg(name, desc):
            try:
                sg = self.ec2.create_security_group(GroupName=name, Description=desc, VpcId=vpc_id)
                return sg["GroupId"]
            except ClientError as exc:
                if exc.response["Error"]["Code"] == "InvalidGroup.Duplicate":
                    existing = self.ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [name]}])
                    return existing["SecurityGroups"][0]["GroupId"]
                raise
            
        sgs['alb'] = get_or_create_sg("dijkfood-alb-sg", "SG para Load Balancer")
        sgs['api'] = get_or_create_sg("dijkfood-api-sg", "SG para ECS API")
        sgs['worker'] = get_or_create_sg("dijkfood-worker-sg", "SG para ECS Worker")
        sgs['rds'] = get_or_create_sg("dijkfood-rds-sg", "SG para RDS")
    
        try: self.ec2.authorize_security_group_ingress(GroupId=sgs['alb'], IpPermissions=[{"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}])
        except ClientError: pass
        try: self.ec2.authorize_security_group_ingress(GroupId=sgs['api'], IpPermissions=[{"IpProtocol": "tcp", "FromPort": API_PORT, "ToPort": API_PORT, "UserIdGroupPairs": [{"GroupId": sgs['alb']}]}])
        except ClientError: pass
        try:
            self.ec2.authorize_security_group_ingress(GroupId=sgs['rds'], IpPermissions=[
                {"IpProtocol": "tcp", "FromPort": DB_PORT, "ToPort": DB_PORT, "UserIdGroupPairs": [{"GroupId": sgs['api']}]},
                {"IpProtocol": "tcp", "FromPort": DB_PORT, "ToPort": DB_PORT, "UserIdGroupPairs": [{"GroupId": sgs['worker']}]},
                {"IpProtocol": "tcp", "FromPort": DB_PORT, "ToPort": DB_PORT, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}
            ])
        except ClientError: pass
    
        return sgs, vpc_id
    
    def create_parameter_group(self):
        try:
            self.rds.create_db_parameter_group(DBParameterGroupName=PG_GROUP_NAME, DBParameterGroupFamily=f"postgres{PG_VERSION}", Description="DijkFood parameter group")
        except ClientError: pass
        return PG_GROUP_NAME

    def allocate_rds(self):
        print(f"[RDS] Creating '{DB_INSTANCE_ID}' Multi-AZ ...")
        try:
            self.rds.create_db_instance(
                DBInstanceIdentifier=DB_INSTANCE_ID, DBInstanceClass=INSTANCE_CLASS, Engine="postgres",
                EngineVersion=PG_VERSION, MasterUsername=DB_ADMIN_USER, MasterUserPassword=DB_PASSWORD,
                DBName=DB_NAME, AllocatedStorage=20, StorageType="gp2", VpcSecurityGroupIds=[self.sgs['rds']],
                DBParameterGroupName=self.pg_group, PubliclyAccessible=True, BackupRetentionPeriod=1, MultiAZ=True 
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'DBInstanceAlreadyExists': raise

        print(f"[RDS] Aguardando o banco ficar 'Available'...")
        self.rds.get_waiter("db_instance_available").wait(DBInstanceIdentifier=DB_INSTANCE_ID, WaiterConfig={"Delay": 30, "MaxAttempts": 40})
        self.rds_endpoint = self.rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)["DBInstances"][0]["Endpoint"]["Address"]
        print(f"[RDS] Endpoint: {self.rds_endpoint}")

    def create_rds_schema(self):
        print(f"\n[RDS] Criando tabelas (Schema)...")
        conn = None
        for attempt in range(1, 7):
            try:
                conn = psycopg2.connect(host=self.rds_endpoint, port=DB_PORT, dbname=DB_NAME, user=DB_ADMIN_USER, password=DB_PASSWORD, connect_timeout=10)
                break
            except psycopg2.OperationalError as exc:
                if attempt == 6: raise
                time.sleep(10)
        
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clientes (
                    id_cliente SERIAL PRIMARY KEY, nome VARCHAR(80), email VARCHAR(80), telefone VARCHAR(20), latitude DOUBLE PRECISION, longitude DOUBLE PRECISION
                );
                CREATE TABLE IF NOT EXISTS restaurantes (
                    id_restaurante SERIAL PRIMARY KEY, nome VARCHAR(80), tipo_cozinha VARCHAR(40), latitude DOUBLE PRECISION, longitude DOUBLE PRECISION
                );
                CREATE TABLE IF NOT EXISTS entregadores (
                    id_entregador SERIAL PRIMARY KEY, nome VARCHAR(80), tipo_veiculo VARCHAR(30), status VARCHAR(30) DEFAULT 'AVAILABLE', latitude DOUBLE PRECISION, longitude DOUBLE PRECISION
                );
                CREATE TABLE IF NOT EXISTS pedidos (
                    id_pedido SERIAL PRIMARY KEY, id_cliente INT REFERENCES clientes(id_cliente), id_restaurante INT REFERENCES restaurantes(id_restaurante), id_entregador INT REFERENCES entregadores(id_entregador), lista_itens TEXT, status VARCHAR(30) DEFAULT 'CONFIRMED', data DATE, horario TIME
                );
            """)
        conn.commit()
        conn.close()

    def allocate_dynamodb(self):
        for tb_name, pk_name in [(DDB_TABLE_EVENTOS, "id_pedido"), (DDB_TABLE_TELEMETRIA, "id_entregador"), (DDB_TABLE_ALOCACAO_ENTREGADOR, "id_entregador")]:
            try:
                table = self.ddb.create_table(TableName=tb_name, KeySchema=[{"AttributeName": pk_name, "KeyType": "HASH"}, {"AttributeName": "timestamp", "KeyType": "RANGE"}], AttributeDefinitions=[{"AttributeName": pk_name, "AttributeType": "S"}, {"AttributeName": "timestamp", "AttributeType": "S"}], BillingMode="PAY_PER_REQUEST")
                table.wait_until_exists()
            except ClientError: pass

    def allocate_s3(self):
        try: self.s3.create_bucket(Bucket=S3_BUCKET_NAME)
        except ClientError: pass

    def allocate_ecs_services(self):
        print(f"\n--- Criando ECS e Load Balancer ---")
        subnets = [s['SubnetId'] for s in self.ec2.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [self.vpc_id]}])['Subnets']]
        self.ecs.create_cluster(clusterName=ECS_CLUSTER_NAME)

        # 1. LOAD BALANCER
        alb_arn, alb_dns = None, None
        try:
            alb = self.elbv2.create_load_balancer(Name=ALB_NAME, Subnets=subnets, SecurityGroups=[self.sgs['alb']], Scheme='internet-facing')
            alb_arn, alb_dns = alb['LoadBalancers'][0]['LoadBalancerArn'], alb['LoadBalancers'][0]['DNSName']
        except ClientError as e:
            if e.response['Error']['Code'] == 'DuplicateLoadBalancerName':
                info = self.elbv2.describe_load_balancers(Names=[ALB_NAME])['LoadBalancers'][0]
                alb_arn, alb_dns = info['LoadBalancerArn'], info['DNSName']
            else: raise

        api_url = f"http://{alb_dns}"
        print(f"[ALB] Load Balancer gerado: {api_url}")

        tg_arn = None
        try:
            tg = self.elbv2.create_target_group(Name=TG_NAME, Protocol='HTTP', Port=API_PORT, VpcId=self.vpc_id, TargetType='ip', HealthCheckPath='/')
            tg_arn = tg['TargetGroups'][0]['TargetGroupArn']
        except ClientError as e:
            if e.response['Error']['Code'] == 'DuplicateTargetGroupName': tg_arn = self.elbv2.describe_target_groups(Names=[TG_NAME])['TargetGroups'][0]['TargetGroupArn']
            else: raise

        if not any(l['Port'] == 80 for l in self.elbv2.describe_listeners(LoadBalancerArn=alb_arn)['Listeners']):
            self.elbv2.create_listener(LoadBalancerArn=alb_arn, Protocol='HTTP', Port=80, DefaultActions=[{'Type': 'forward', 'TargetGroupArn': tg_arn}])

        # 2. SERVIÇO DA API
        try:
            api_task = self.ecs.register_task_definition(
                family="dijkfood-api-task", networkMode="awsvpc", executionRoleArn=LAB_ROLE_ARN, taskRoleArn=LAB_ROLE_ARN, requiresCompatibilities=["FARGATE"], cpu="256", memory="512",
                containerDefinitions=[{
                    "name": "dijkfood-api-container", "image": "wallita/dijkfood-api:latest", 
                    "portMappings": [{"containerPort": API_PORT, "hostPort": API_PORT, "protocol": "tcp"}],
                    "environment": [{"name": "DB_HOST", "value": self.rds_endpoint}, {"name": "DB_USER", "value": DB_ADMIN_USER}, {"name": "DB_PASS", "value": DB_PASSWORD}, {"name": "DB_NAME", "value": DB_NAME}, {"name": "DDB_EVENTOS", "value": DDB_TABLE_EVENTOS}, {"name": "DDB_TELEMETRIA", "value": DDB_TABLE_TELEMETRIA}],
                    "logConfiguration": {"logDriver": "awslogs", "options": {"awslogs-group": "/ecs/dijkfood-api", "awslogs-region": REGION, "awslogs-stream-prefix": "ecs", "awslogs-create-group": "true"}}
                }]
            )
            self.ecs.create_service(
                cluster=ECS_CLUSTER_NAME, serviceName="dijkfood-api-service", taskDefinition=api_task['taskDefinition']['taskDefinitionArn'], desiredCount=2, launchType="FARGATE",
                networkConfiguration={'awsvpcConfiguration': {'subnets': subnets, 'securityGroups': [self.sgs['api']], 'assignPublicIp': 'ENABLED'}},
                loadBalancers=[{'targetGroupArn': tg_arn, 'containerName': 'dijkfood-api-container', 'containerPort': API_PORT}]
            )
        except ClientError: pass

        # 3. SERVIÇO DO WORKER (Com API_URL dinâmica!)
        try:
            worker_task = self.ecs.register_task_definition(
                family="dijkfood-worker-task", networkMode="awsvpc", executionRoleArn=LAB_ROLE_ARN, taskRoleArn=LAB_ROLE_ARN, requiresCompatibilities=["FARGATE"], cpu="256", memory="512",
                containerDefinitions=[{
                    "name": "dijkfood-worker-container", "image": "matheuspinto4/dijkfood-worker:latest",
                    "environment": [
                        {"name": "DB_HOST", "value": self.rds_endpoint}, {"name": "DB_USER", "value": DB_ADMIN_USER}, {"name": "DB_PASS", "value": DB_PASSWORD}, {"name": "DB_NAME", "value": DB_NAME}, {"name": "S3_BUCKET", "value": S3_BUCKET_NAME},
                        {"name": "API_URL", "value": api_url} # <--- Injeção Dinâmica
                    ],
                    "logConfiguration": {"logDriver": "awslogs", "options": {"awslogs-group": "/ecs/dijkfood-worker", "awslogs-region": REGION, "awslogs-stream-prefix": "ecs", "awslogs-create-group": "true"}}
                }]
            )
            self.ecs.create_service(
                cluster=ECS_CLUSTER_NAME, serviceName="dijkfood-worker-service", taskDefinition=worker_task['taskDefinition']['taskDefinitionArn'], desiredCount=1, launchType="FARGATE",
                networkConfiguration={'awsvpcConfiguration': {'subnets': subnets, 'securityGroups': [self.sgs['worker']], 'assignPublicIp': 'ENABLED'}}
            )
        except ClientError: pass

    def populate_s3(self):
        print(f"\n[POPULATE] Gerando grafo de '{PLACE_NAME}'...")
        G = ox.graph_from_place(PLACE_NAME, network_type=NETWORK_TYPE)
        
        with open("graph_nodes.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["node_id", "lat", "lon"])
            for node_id, data in G.nodes(data=True): writer.writerow([node_id, data['y'], data['x']])
                
        with open("graph_edges.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["from_node", "to_node", "weight"])
            for u, v, data in G.edges(data=True): writer.writerow([u, v, data.get('length', 1.0)])

        self.s3.upload_file("graph_nodes.csv", S3_BUCKET_NAME, "graph_nodes.csv")
        self.s3.upload_file("graph_edges.csv", S3_BUCKET_NAME, "graph_edges.csv")
        print(f"[POPULATE] Grafo salvo no S3!")

    def destroy(self):
        print("\n[DESTROY] Iniciando limpeza...")
        
        try:
            self.ecs.update_service(cluster=ECS_CLUSTER_NAME, service="dijkfood-api-service", desiredCount=0)
            self.ecs.delete_service(cluster=ECS_CLUSTER_NAME, service="dijkfood-api-service")
            self.ecs.update_service(cluster=ECS_CLUSTER_NAME, service="dijkfood-worker-service", desiredCount=0)
            self.ecs.delete_service(cluster=ECS_CLUSTER_NAME, service="dijkfood-worker-service")
        except ClientError: pass

        try:
            alb_arn = self.elbv2.describe_load_balancers(Names=[ALB_NAME])['LoadBalancers'][0]['LoadBalancerArn']
            self.elbv2.delete_load_balancer(LoadBalancerArn=alb_arn)
            tg_arn = self.elbv2.describe_target_groups(Names=[TG_NAME])['TargetGroups'][0]['TargetGroupArn']
            self.elbv2.delete_target_group(TargetGroupArn=tg_arn)
        except ClientError: pass

        try:
            self.rds.delete_db_instance(DBInstanceIdentifier=DB_INSTANCE_ID, SkipFinalSnapshot=True)
        except ClientError: pass

        try: self.ddb.Table(DDB_TABLE_EVENTOS).delete()
        except ClientError: pass
        try: self.ddb.Table(DDB_TABLE_TELEMETRIA).delete()
        except ClientError: pass

        try:
            objects = self.s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
            if 'Contents' in objects:
                for obj in objects['Contents']: self.s3.delete_object(Bucket=S3_BUCKET_NAME, Key=obj['Key'])
            self.s3.delete_bucket(Bucket=S3_BUCKET_NAME)
        except ClientError: pass

        print("[DESTROY] Comandos de exclusão enviados para a AWS!")

def main():
    parser = argparse.ArgumentParser(description="Deploy Architecture DijkFood")
    parser.add_argument("--step", choices=["allocate", "populate", "destroy", "simulator"], help="Step to run")
    args = parser.parse_args()

    arq = Arquitetura()

    if args.step == "allocate":
        arq.allocate()
    elif args.step == "populate":
        arq.populate_s3()
    elif args.step == "destroy":
        arq.destroy()
    elif args.step == "simulator":
        asyncio.run(simulator())
    else:
        arq.allocate()
        arq.populate_s3()
        arq.destroy()

if __name__ == "__main__":
    main()
"""
deploy.py — Full Architecture Deployment & Traffic Simulator
Criado para o projeto DijkFood

Lifecycle:
  1. allocate   — Cria toda a infraestrutura e as tabelas vazias no Banco de Dados
  2. populate   — Baixa o Grafo Viário (OSMnx) e envia para o S3
  3. destroy    — Apaga rigorosamente TODOS os recursos
  4. simulator  — Roda a simulação de tráfego usando a URL dinâmica do ALB
"""

import argparse
import boto3
import psycopg2
import time
import csv
import os
import osmnx as ox
from botocore.exceptions import ClientError
import asyncio
import httpx
import statistics
import random

from fake_data import gerar_dados_falsos

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES DA INFRAESTRUTURA
# ─────────────────────────────────────────────────────────────────────────────
REGION         = "us-east-1"
try:
    ACCOUNT_ID = boto3.client('sts').get_caller_identity().get('Account')
    LAB_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/LabRole"
except Exception:
    ACCOUNT_ID = "000000000000"
    LAB_ROLE_ARN = ""

DB_INSTANCE_ID = "dijkfood-primary"
DB_NAME        = "dijkfooddb"
DB_ADMIN_USER  = "dijk_admin"
DB_PASSWORD    = "DijkFood2026!Cloud"  
DB_PORT        = 5432
INSTANCE_CLASS = "db.t3.micro"
PG_VERSION     = "16"
PG_GROUP_NAME  = "dijkfood-pg16"   

DDB_TABLE_EVENTOS    = "dijkfood-historico-eventos"
DDB_TABLE_TELEMETRIA = "dijkfood-telemetria-entregadores"
DDB_TABLE_ALOCACOES = "dijkfood-alocacao-entregadores"

S3_BUCKET_NAME = f"dijkfood-grafo-sp-{ACCOUNT_ID}"
PLACE_NAME     = "São Paulo, Brazil"
NETWORK_TYPE   = "drive"

ECS_CLUSTER_NAME = "DijkFoodCluster"
ALB_NAME         = "dijkfood-api-alb"
TG_NAME          = "dijkfood-api-tg"
API_PORT         = 80 


CPU_WORKER = 1024
MEMORY_WORKER = 2048
CPU_API = 1024
MEMORY_API = 2048

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES DO SIMULADOR
# ─────────────────────────────────────────────────────────────────────────────
# MUDANÇA: Aumentado para haver tráfego de verdade com o limite de 1 pedido por cliente
N_CLIENTES     = 20  
N_RESTAURANTES = 20 
N_ENTREGADORES = 20
CONCURRENCY = 200
VELOCIDADE_KMH = 2000
fator = 18 / (VELOCIDADE_KMH * 0.1) 


GLOBAL_API_URL = "" 

VOLUMES = {
    "OPERACAO_NORMAL": 100,
    "PICO": 500,
    "EVENTO_ESPECIAL": 2000
}
RITMO_EXEC = [
    {
        "volume": "OPERACAO_NORMAL",
        "duracao": 300 
    },
    {
        "volume": "PICO",
        "duracao": 300
    },
    {
        "volume": "EVENTO_ESPECIAL",
        "duracao": 300 
    }
]

# Travas Assíncronas (Locks) do Simulador
orders = {}
orders_lock = asyncio.Lock()
entregadores_desocupados = {}
entregadores_desocupados_lock = asyncio.Lock()
entregadores_ocupados = {}
entregadores_ocupados_lock = asyncio.Lock()

# --- NOVO: Sistema de fila de clientes ---
clientes_esperando = set()
clientes_lock = asyncio.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# MÓDULO DE ARQUITETURA
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
    
    def get_api_url(self):
        """Busca o DNS do Load Balancer dinamicamente na AWS."""
        try:
            info = self.elbv2.describe_load_balancers(Names=[ALB_NAME])['LoadBalancers'][0]
            return f"http://{info['DNSName']}"
        except ClientError:
            print("\n[ERRO] Load Balancer não encontrado! Tem certeza que você já rodou '--step allocate'?")
            return None

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
        for tb_name, pk_name in [(DDB_TABLE_EVENTOS, "id_pedido"), (DDB_TABLE_TELEMETRIA, "id_entregador"), (DDB_TABLE_ALOCACOES, "id_entregador")]:
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
                family="dijkfood-api-task", networkMode="awsvpc", executionRoleArn=LAB_ROLE_ARN, taskRoleArn=LAB_ROLE_ARN, requiresCompatibilities=["FARGATE"], cpu=f"{CPU_API}", memory=f"{MEMORY_API}",
                containerDefinitions=[{
                    "name": "dijkfood-api-container", "image": "matheuspinto4/dijkfood-api:latest", 
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
            
            print("[ECS] Configurando Auto Scaling para a API...")
            try:
                self.app_asg.register_scalable_target(
                    ServiceNamespace='ecs',
                    ResourceId=f'service/{ECS_CLUSTER_NAME}/dijkfood-api-service',
                    ScalableDimension='ecs:service:DesiredCount',
                    MinCapacity=2,
                    MaxCapacity=10 
                )
                self.app_asg.put_scaling_policy(
                    PolicyName='api-cpu-scaling',
                    ServiceNamespace='ecs',
                    ResourceId=f'service/{ECS_CLUSTER_NAME}/dijkfood-api-service',
                    ScalableDimension='ecs:service:DesiredCount',
                    PolicyType='TargetTrackingScaling',
                    TargetTrackingScalingPolicyConfiguration={
                        'TargetValue': 50.0, 
                        'PredefinedMetricSpecification': {'PredefinedMetricType': 'ECSServiceAverageCPUUtilization'},
                        'ScaleOutCooldown': 60,
                        'ScaleInCooldown': 60
                    }
                )
            except ClientError as e:
                print(f"Aviso AutoScaling API: {e}")
                
        except ClientError as e: 
            print(f"\n❌ [ERRO FATAL ECS] Falha ao criar a API: {e}\n")

        # 3. SERVIÇO DO WORKER
        try:
            worker_task = self.ecs.register_task_definition(
                family="dijkfood-worker-task", networkMode="awsvpc", executionRoleArn=LAB_ROLE_ARN, taskRoleArn=LAB_ROLE_ARN, requiresCompatibilities=["FARGATE"], cpu=f"{CPU_WORKER}", memory=f"{MEMORY_WORKER}",
                containerDefinitions=[{
                    "name": "dijkfood-worker-container", "image": "matheuspinto4/dijkfood-worker:latest",
                    "environment": [
                        {"name": "DB_HOST", "value": self.rds_endpoint}, 
                        {"name": "DB_USER", "value": DB_ADMIN_USER}, 
                        {"name": "DB_PASS", "value": DB_PASSWORD}, 
                        {"name": "DB_NAME", "value": DB_NAME}, 
                        {"name": "S3_BUCKET", "value": S3_BUCKET_NAME},
                        {"name": "API_URL", "value": api_url}, 
                        {"name": "DDB_ALOCACOES", "value": DDB_TABLE_ALOCACOES}
                    ],
                    "logConfiguration": {"logDriver": "awslogs", "options": {"awslogs-group": "/ecs/dijkfood-worker", "awslogs-region": REGION, "awslogs-stream-prefix": "ecs", "awslogs-create-group": "true"}}
                }]
            )
            self.ecs.create_service(
                cluster=ECS_CLUSTER_NAME, serviceName="dijkfood-worker-service", taskDefinition=worker_task['taskDefinition']['taskDefinitionArn'], desiredCount=1, launchType="FARGATE",
                networkConfiguration={'awsvpcConfiguration': {'subnets': subnets, 'securityGroups': [self.sgs['worker']], 'assignPublicIp': 'ENABLED'}}
            )
            
            print("[ECS] Configurando Auto Scaling para o Worker...")
            try:
                self.app_asg.register_scalable_target(
                    ServiceNamespace='ecs',
                    ResourceId=f'service/{ECS_CLUSTER_NAME}/dijkfood-worker-service',
                    ScalableDimension='ecs:service:DesiredCount',
                    MinCapacity=2,
                    MaxCapacity=10 
                )
                self.app_asg.put_scaling_policy(
                    PolicyName='worker-cpu-scaling',
                    ServiceNamespace='ecs',
                    ResourceId=f'service/{ECS_CLUSTER_NAME}/dijkfood-worker-service',
                    ScalableDimension='ecs:service:DesiredCount',
                    PolicyType='TargetTrackingScaling',
                    TargetTrackingScalingPolicyConfiguration={
                        'TargetValue': 50.0, 
                        'PredefinedMetricSpecification': {'PredefinedMetricType': 'ECSServiceAverageCPUUtilization'},
                        'ScaleOutCooldown': 60,
                        'ScaleInCooldown': 60
                    }
                )
            except ClientError as e:
                print(f"Aviso AutoScaling Worker: {e}")
        except ClientError as e: 
            print(f"\n❌ [ERRO FATAL ECS] Falha ao criar o Worker: {e}\n")

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

    def destroy_ecs_only(self):
        print("\n[DESTROY ECS] Limpando apenas os contêineres ECS e Load Balancer...")
        
        print("[DESTROY ECS] Apagando API...")
        try:
            self.ecs.update_service(cluster=ECS_CLUSTER_NAME, service="dijkfood-api-service", desiredCount=0)
            self.ecs.delete_service(cluster=ECS_CLUSTER_NAME, service="dijkfood-api-service")
        except ClientError: pass 
        
        try:
            waiter_api = self.ecs.get_waiter('services_inactive')
            waiter_api.wait(
                cluster=ECS_CLUSTER_NAME, 
                services=["dijkfood-api-service"],
                WaiterConfig={'Delay': 15, 'MaxAttempts': 40}
            )
            print(" -> API desligada com sucesso!")
        except Exception: pass

        print("[DESTROY ECS] Apagando Worker...")
        try:
            self.ecs.update_service(cluster=ECS_CLUSTER_NAME, service="dijkfood-worker-service", desiredCount=0)
            self.ecs.delete_service(cluster=ECS_CLUSTER_NAME, service="dijkfood-worker-service")
        except ClientError: pass
        
        try:
            waiter_worker = self.ecs.get_waiter('services_inactive')
            waiter_worker.wait(
                cluster=ECS_CLUSTER_NAME, 
                services=["dijkfood-worker-service"],
                WaiterConfig={'Delay': 15, 'MaxAttempts': 40}
            )
            print(" -> Worker desligado com sucesso!")
        except Exception: pass

        try:
            alb_arn = self.elbv2.describe_load_balancers(Names=[ALB_NAME])['LoadBalancers'][0]['LoadBalancerArn']
            self.elbv2.delete_load_balancer(LoadBalancerArn=alb_arn)
            tg_arn = self.elbv2.describe_target_groups(Names=[TG_NAME])['TargetGroups'][0]['TargetGroupArn']
            self.elbv2.delete_target_group(TargetGroupArn=tg_arn)
        except ClientError: pass
        
        print("[DESTROY ECS] ECS limpo! O RDS e DynamoDB continuam intactos.")

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
            print("[DESTROY] Aguardando o RDS ser deletado (5-10 min)...")
            self.rds.get_waiter('db_instance_deleted').wait(DBInstanceIdentifier=DB_INSTANCE_ID, WaiterConfig={"Delay": 30, "MaxAttempts": 40})
        except ClientError: pass

        try: self.ddb.Table(DDB_TABLE_EVENTOS).delete()
        except ClientError: pass
        try: self.ddb.Table(DDB_TABLE_TELEMETRIA).delete()
        except ClientError: pass
        try: self.ddb.Table(DDB_TABLE_ALOCACOES).delete()
        except ClientError: pass

        try:
            objects = self.s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
            if 'Contents' in objects:
                for obj in objects['Contents']: self.s3.delete_object(Bucket=S3_BUCKET_NAME, Key=obj['Key'])
            self.s3.delete_bucket(Bucket=S3_BUCKET_NAME)
        except ClientError: pass

        print("[DESTROY] Comandos de exclusão enviados para a AWS!")


# ─────────────────────────────────────────────────────────────────────────────
# MÓDULO DO SIMULADOR DE TRÁFEGO
# ─────────────────────────────────────────────────────────────────────────────
async def preload(jsons, url):
    print(f"Enviando lote de {len(jsons)} registros para {url}bulk ...")
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(url + "bulk", json=jsons)
        if response.status_code == 200:
            print(f"Sucesso! {len(jsons)} inseridos.")
        else:
            print(f"Erro fatal no Bulk Insert: {response.text}")
            raise Exception(f"Falha ao popular o banco de dados!, {url}bulk, {jsons}")

async def requester(queue, results):
    def alloc_courier(entregador_dict, id_pedido, rota_restaurante, rota_cliente):
        entregador_dict["id_pedido"] = id_pedido
        entregador_dict["edge_idx"] = 0
        entregador_dict["rota_restaurante"] = rota_restaurante
        entregador_dict["rota_cliente"] = rota_cliente
        entregador_dict["rota_atual"] = "rota_restaurante"
        entregador_dict["deslocamento_atual"] = [*entregador_dict["posicao"], 0]
        return entregador_dict 

    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                break
            start = time.perf_counter()
            try:
                if item["method"] == "GET":
                    response = await client.get(item["url"])
                    if response.status_code == 200:
                        dados = response.json()
                        id_pedido = dados.get("id_pedido", None)
                        id_entregador = dados.get("id_entregador", None)
                        rota_restaurante = dados.get("rota_restaurante", None)
                        rota_cliente = dados.get("rota_cliente", None)
                        
                        if item["user"] == "courier" and id_pedido and id_entregador:
                            async with entregadores_desocupados_lock:
                                if id_entregador in entregadores_desocupados: 
                                    entregador_dict = entregadores_desocupados[id_entregador]
                                    del entregadores_desocupados[id_entregador]
                                    
                                    async with entregadores_ocupados_lock:
                                        entregadores_ocupados[id_entregador] = alloc_courier(entregador_dict, id_pedido, rota_restaurante, rota_cliente)

                elif item["method"] in ("POST", "PATCH"):
                    if item["method"] == "POST":
                        response = await client.post(item["url"], json=item["json"])
                        
                        # NOVO: Registra o id do cliente se der sucesso, ou libera se falhar
                        if item["user"] == "client" and item["url"].endswith("/pedidos/"):
                            if response.status_code == 200:
                                pedido_criado = response.json()
                                async with orders_lock:
                                    orders[pedido_criado["id_pedido"]] = {
                                        "status": "CONFIRMED",
                                        "id_cliente": item["json"]["id_cliente"]
                                    }
                            else:
                                async with clientes_lock:
                                    clientes_esperando.discard(item["json"]["id_cliente"])
                    else:  
                        response = await client.patch(item["url"], json=item["json"])
                    
                    if item["user"] == "courier" and item["json"].get("novo_status", "") == "PICKED_UP":
                        await queue.put({
                            "method": "PATCH",
                            "url": item["url"],
                            "json": {"novo_status": "IN_TRANSIT", "id_entregador": item["json"].get("id_entregador")}, 
                            "ritmo_idx": item["ritmo_idx"],
                            "user": "courier"
                        })
                
                latency = time.perf_counter() - start
                results.append({
                    "status": response.status_code if 'response' in locals() else "desconhecido",
                    "latency": latency,
                    "ritmo_idx": item["ritmo_idx"],
                    "method": item["method"],
                    "user": item["user"]
                })

            except Exception as e:
                # NOVO: Libera o cliente caso o POST do pedido falhe por erro de rede
                if item["method"] == "POST" and item["user"] == "client" and item["url"].endswith("/pedidos/"):
                    async with clientes_lock:
                        clientes_esperando.discard(item["json"]["id_cliente"])
                        
                latency = time.perf_counter() - start
                results.append({
                    "status": "error",
                    "latency": latency,
                    "error": str(e),
                    "ritmo_idx": item["ritmo_idx"],
                    "method": item["method"],
                    "user": item["user"]
                })

            queue.task_done()

async def restaurant_updater(queue, volume, duracao, ritmo_idx):
    volume = max(volume // 6, 1)
    start = time.perf_counter()
    while time.perf_counter() - start < duracao:
        current_start = time.perf_counter()
        delta = random.expovariate(volume)
        
        status = "CONFIRMED"
        novo_status = "PREPARING"
        if random.random() <= 0.1:
            status = "PREPARING"
            novo_status = "READY_FOR_PICKUP"
        
        try:
            async with orders_lock:
                current_orders = dict(orders)
            id_pedido = next(pedido_num for pedido_num, pedido in current_orders.items() if pedido["status"] == status)
        except Exception as e:
            await asyncio.sleep(0.01)
            continue
        
        await queue.put({
            "method": "PATCH",
            "url": f"{GLOBAL_API_URL}/pedidos/{id_pedido}/status",
            "json": {"novo_status": novo_status}, 
            "ritmo_idx": ritmo_idx,
            "user": "restaurant"
        })
        async with orders_lock:
            orders[id_pedido]["status"] = novo_status
            
        delta_sleep = current_start + delta - time.perf_counter()
        await asyncio.sleep(max(delta_sleep, 0))

async def move_courier(id_entregador):
    fim_rota = False 
    
    def compute_new_desl(entregador):
        edge_idx = entregador["edge_idx"]
        rota_atual = entregador["rota_atual"]
        
        if not entregador[rota_atual]:
            return 0
            
        if edge_idx >= len(entregador[rota_atual]): return 1
        
        coord_atual, coord_nova, metros = entregador[rota_atual][edge_idx]
        lat, long = float(coord_atual[0]), float(coord_atual[1])
        new_lat, new_long = float(coord_nova[0]), float(coord_nova[1])
        metros = float(metros)
        
        dist_100ms = max(1, int((fator/10) * metros + 0.5))
        lat_desl = (new_lat - lat) / dist_100ms
        long_desl = (new_long - long) / dist_100ms
        
        entregador["deslocamento_atual"] = [lat_desl, long_desl, dist_100ms]
        entregador["posicao"] = lat, long
        entregador["edge_idx"] += 1
        return 0

    async with entregadores_ocupados_lock:
        couriers = dict(entregadores_ocupados)

    entregador = couriers.get(id_entregador, None)
    if not entregador:
        return None, None, None
        
    if entregador["deslocamento_atual"][2] <= 0: 
        fim_rota = compute_new_desl(entregador)
        
    lat, long = entregador["posicao"]
    lat_desl, long_desl, dist_100ms = entregador["deslocamento_atual"] 
    
    if dist_100ms > 0:   
        entregador["posicao"] = lat + lat_desl, long + long_desl 
        entregador["deslocamento_atual"][2] = dist_100ms - 1
        mudanca_rota = False
    elif fim_rota:
        rota_atual = entregador["rota_atual"]
        if rota_atual == "rota_restaurante":
            id_pedido = int(entregador["id_pedido"])
            
            async with orders_lock:
                if id_pedido not in orders:
                    return lat, long, None 
                status = orders[id_pedido]["status"]
                
            if status != "READY_FOR_PICKUP":
                return lat, long, None
            
            entregador["rota_atual"] = "rota_cliente"
            entregador["edge_idx"] = 0
            async with orders_lock:
                orders[id_pedido]["status"] = "IN_TRANSIT"
            mudanca_rota = True
        elif rota_atual == "rota_cliente":
            return None, None, None
    
    async with entregadores_ocupados_lock:
        entregadores_ocupados[id_entregador] = entregador

    return entregador["posicao"][0], entregador["posicao"][1], mudanca_rota

async def updater_courier(queue, volume, duracao, ritmo_idx):
    def desalloc_courier(entregador_dict):
        entregador_dict["id_pedido"] = None
        entregador_dict["edge_idx"] = None
        entregador_dict["rota_restaurante"] = None
        entregador_dict["rota_cliente"] = None
        entregador_dict["rota_atual"] = None
        entregador_dict["deslocamento_atual"] = None
        return entregador_dict 
        
    start = time.perf_counter()
    current_100ms = 0.0
    
    while time.perf_counter() - start < duracao:
        try:
            couriers = None
            async with entregadores_ocupados_lock:
                couriers = dict(entregadores_ocupados)
            
            if not couriers:
                await asyncio.sleep(0.01)
                continue

            for id_entregador in list(couriers.keys()):
                lat, long, mudanca_rota = await move_courier(id_entregador)
                id_pedido = int(couriers[id_entregador]["id_pedido"])
                
                if not lat or not long: 
                    await queue.put({"method": "POST", "url": f"{GLOBAL_API_URL}/alocacoes/{id_entregador}/desativar/{id_pedido}", "json": {}, "ritmo_idx": ritmo_idx, "user": "courier"})
                    await queue.put({"method": "PATCH", "url": f"{GLOBAL_API_URL}/pedidos/{id_pedido}/status", "json": {"novo_status": "DELIVERED", "id_entregador": id_entregador}, "ritmo_idx": ritmo_idx, "user": "courier"})
                    
                    async with orders_lock:
                        if id_pedido in orders: 
                            # NOVO: Descobre quem era o cliente e risca o nome dele da lista de espera
                            id_cli_do_pedido = orders[id_pedido].get("id_cliente")
                            del orders[id_pedido]
                            if id_cli_do_pedido:
                                async with clientes_lock:
                                    clientes_esperando.discard(id_cli_do_pedido)

                    async with entregadores_ocupados_lock:
                        entregador_dict = entregadores_ocupados[id_entregador]
                        del entregadores_ocupados[id_entregador]
                    async with entregadores_desocupados_lock:
                        entregadores_desocupados[id_entregador] = desalloc_courier(entregador_dict)
                else:
                    json_data = {"latitude": lat, "longitude": long}
                    await queue.put({"method": "POST", "url": f"{GLOBAL_API_URL}/entregadores/{id_entregador}/posicao", "json": json_data, "ritmo_idx": ritmo_idx, "user": "courier"})
                        
                    if mudanca_rota:
                        await queue.put({"method": "PATCH", "url": f"{GLOBAL_API_URL}/pedidos/{id_pedido}/status", "json": {"novo_status": "PICKED_UP", "id_entregador": id_entregador}, "ritmo_idx": ritmo_idx, "user": "courier"})

        except Exception as e:
            print(f"\n[ERRO CRÍTICO NO SIMULADOR] O entregador travou: {e}")

        current_100ms += 0.1
        delta = start + current_100ms - time.perf_counter()
        await asyncio.sleep(max(delta, 0))

async def viewer_courier(queue, volume, duracao, ritmo_idx):
    start = time.perf_counter()
    current_second = 0
    volume *= 3

    times = []
    while len(times) < volume:
        delta = random.expovariate(volume)
        if delta >= 1: continue
        times.append(delta)
    times = sorted(times)
    while time.perf_counter() - start < duracao:
        for each_time in times:
            couriers = None
            async with entregadores_desocupados_lock:
                couriers = list(entregadores_desocupados.keys())
            
            if not couriers:
                await asyncio.sleep(0.01)
                continue
            
            id_entregador = random.choice(couriers)

            delta = start + current_second + each_time - time.perf_counter()
            await asyncio.sleep(max(delta, 0))

            await queue.put({
                "method": "GET",
                "url": f"{GLOBAL_API_URL}/alocacoes/{id_entregador}/acompanhamento",
                "params": {},
                "ritmo_idx": ritmo_idx,
                "user": "courier"
            })

        current_second += 1
        delta = start + current_second - time.perf_counter()
        await asyncio.sleep(max(delta, 0))

async def producer_order(queue, volume, duracao, ritmo_idx, clientes, restaurantes):
    start = time.perf_counter()

    while time.perf_counter() - start < duracao:
        current_start = time.perf_counter()
        delta = random.expovariate(volume)

        # NOVO: Separa quem pode pedir (quem não está no caderninho)
        async with clientes_lock:
            livres = list(set(clientes) - clientes_esperando)
        
        if not livres:
            await asyncio.sleep(0.01)
            continue

        id_cli = random.choice(livres)
        
        # Anota o nome dele antes de disparar na API
        async with clientes_lock:
            clientes_esperando.add(id_cli)

        id_res = random.choice(restaurantes)
        json_data = {"id_cliente": id_cli, "id_restaurante": id_res, "lista_itens": []}

        await queue.put({
            "method": "POST",
            "url": f"{GLOBAL_API_URL}/pedidos/",
            "json": json_data, 
            "ritmo_idx": ritmo_idx,
            "user": "client"
        })
        
        delta_sleep = current_start + delta - time.perf_counter()
        await asyncio.sleep(max(delta_sleep, 0))

async def viewer_order(queue, volume, duracao, ritmo_idx):
    start = time.perf_counter()
    await asyncio.sleep(start + 1 - time.perf_counter())
    
    while time.perf_counter() - start < duracao + 1:
        current_start = time.perf_counter()
        delta = random.expovariate(volume)
        
        try:
            async with orders_lock:
                current_orders = dict(orders)
            order_id = random.choice(list(current_orders.keys()))
        except Exception as e:
            await asyncio.sleep(0.01)
            continue
          
        await queue.put({
            "method": "GET",
            "url": f"{GLOBAL_API_URL}/pedidos/{order_id}/acompanhamento",
            "params": {}, 
            "ritmo_idx": ritmo_idx,
            "user": "client"
        })
        
        delta_sleep = current_start + delta - time.perf_counter()
        await asyncio.sleep(max(delta_sleep, 0))

async def run_simulation():
    print(f"\n[SIMULADOR] Iniciando tráfego contra a API: {GLOBAL_API_URL}")
    
    ids = {}
    data = gerar_dados_falsos(
        numero_de_clientes=N_CLIENTES, 
        numero_de_restaurantes=N_RESTAURANTES, 
        numero_de_entregadores=N_ENTREGADORES
    )
    
    tasks = [
        preload(jsons, GLOBAL_API_URL + path)
        for jsons, path in zip(
            data,
            ["/clientes/", "/restaurantes/", "/entregadores/"]
        )
    ]
    await asyncio.gather(*tasks)
    
    clientes = list(range(1, N_CLIENTES+1))
    restaurantes = list(range(1, N_RESTAURANTES+1))
    
    async with entregadores_desocupados_lock:
        for id_entregador, [lat, long] in zip(
            list(range(1, N_ENTREGADORES+1)), 
            [[v["latitude"], v["longitude"]] for v in data[2]]
        ): 
            entregadores_desocupados[id_entregador] = {
                "id_pedido": None,
                "posicao": [lat, long],
                "edge_idx": None,
                "rota_restaurante": None,
                "rota_cliente": None,
                "rota_atual": None,
                "deslocamento_atual": None
            }
    
    queue = asyncio.Queue()
    results = []

    requesters = [
        asyncio.create_task(requester(queue, results))
        for _ in range(CONCURRENCY)
    ]

    for idx in range(len(RITMO_EXEC)):
        ritmo = RITMO_EXEC[idx]
        volume = VOLUMES[ritmo["volume"]]
        duracao = ritmo["duracao"]
        print(f"RITMO {volume} por {duracao} segundos")
        producers = [
            asyncio.create_task(producer_order(queue, volume, duracao, idx, clientes, restaurantes)),
            asyncio.create_task(viewer_order(queue, volume, duracao, idx)),
            asyncio.create_task(updater_courier(queue, volume, duracao, idx)),
            asyncio.create_task(viewer_courier(queue, volume, duracao, idx)),
            asyncio.create_task(restaurant_updater(queue, volume, duracao, idx))
        ]

        await asyncio.gather(*producers)
        await queue.join()
        
    for _ in requesters:
        await queue.put(None)

    await asyncio.gather(*requesters)
    
    def percentil(data, p):
        data_sorted = sorted(data)
        k = int(len(data_sorted) * p / 100)
        return data_sorted[min(k, len(data_sorted) - 1)]

    latencias = {i: {"PATCH": [], "POST": [], "GET": []} for i in range(len(RITMO_EXEC))}
    for r in results:
        ritmo_idx = r["ritmo_idx"]
        latency = r["latency"]
        method = r["method"]
        latencias[ritmo_idx][method].append(latency)

    print("\n===== RESULTADOS DA SIMULAÇÃO =====")
    print(f"Total de requisições: {len(results)}")
    print("\nLatência:")
    
    metrics = {
        "Min": min, 
        "Mean": statistics.mean, 
        "Median": statistics.median, 
        "P95": lambda x: percentil(x, 95), 
        "Max": max,
    }
    seccion = "-" * (len(metrics) * 12 + 32)
    print(f"|{'Volume':^30}|", *[f"{metric:^10}|" for metric in metrics.keys()])
    print(seccion)
    for ritmo_idx, ritmo_latencia in latencias.items():
        ritmo_name = RITMO_EXEC[ritmo_idx]["volume"]
        ritmo_dura = RITMO_EXEC[ritmo_idx]["duracao"]
        prefixos = [f"{ritmo_idx+1}. {ritmo_name}", f"   {ritmo_dura} segundos", ""]
        for prefix, sufix, latencia in zip(prefixos, ritmo_latencia.keys(), ritmo_latencia.values()):
            print(f"| {prefix:<19}", f"{sufix:>9}|", *[f"{round(f(latencia), 4) if latencia != [] else '':^10}|" for f in metrics.values()])
        print(seccion)


# ─────────────────────────────────────────────────────────────────────────────
# CONTROLADOR PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────
def main():
    global GLOBAL_API_URL
    parser = argparse.ArgumentParser(description="Deploy Architecture & Simulator DijkFood")
    parser.add_argument("--step", choices=["allocate", "populate", "destroy", "simulator", "redeploy_ecs"], help="Step to run")
    args = parser.parse_args()

    arq = Arquitetura()

    if args.step == "allocate":
        arq.allocate()
    elif args.step == "populate":
        arq.populate_s3()
    elif args.step == "destroy":
        arq.destroy()
    elif args.step == "redeploy_ecs":
        arq.sgs, arq.vpc_id = arq.create_security_groups()
        arq.rds_endpoint = arq.rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)["DBInstances"][0]["Endpoint"]["Address"]
        arq.destroy_ecs_only()
        print("Aguardando 15s para a AWS liberar as placas de rede do ALB...")
        time.sleep(15)
        arq.allocate_ecs_services()
    elif args.step == "simulator":
        api_url = arq.get_api_url()
        if not api_url:
            return 
        GLOBAL_API_URL = api_url
        asyncio.run(run_simulation())
    else:
        arq.allocate()
        arq.populate_s3()
        arq.destroy()

if __name__ == "__main__":
    main()
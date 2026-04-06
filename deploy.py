"""
deploy.py — Full Architecture Deployment (RDS Multi-AZ, DDB, S3, ECS API, ECS Worker)
Criado para o projeto DijkFood

Restrito aos recursos exigidos: EC2 (usado apenas para SGs/Rede), RDS, DynamoDB, ECS, S3.

Lifecycle:
  1. allocate   — Cria toda a infraestrutura e as tabelas vazias no Banco de Dados
  2. populate   — Baixa o Grafo Viário (OSMnx) e envia para o S3
  3. destroy    — Apaga rigorosamente TODOS os recursos

Usage:
  python deploy_dijkfood.py                           # Cria, popula o S3 e APAGA em seguida
  python deploy_dijkfood.py --step allocate           # Cria a infraestrutura e o schema vazio
  python deploy_dijkfood.py --step populate           # Envia o Grafo para o S3
  python deploy_dijkfood.py --step destroy            # Apaga toda a infraestrutura
"""

import argparse
import boto3
import psycopg2
import time
import csv
import os
import osmnx as ox
from botocore.exceptions import ClientError

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

# Configurações S3 & Grafo
S3_BUCKET_NAME = f"dijkfood-grafo-sp-{ACCOUNT_ID}"
PLACE_NAME     = "São Paulo, Brazil"
NETWORK_TYPE   = "drive"

# Configurações ECS
ECS_CLUSTER_NAME = "DijkFoodCluster"
ALB_NAME         = "dijkfood-api-alb"
TG_NAME          = "dijkfood-api-tg"
API_PORT         = 80 # A porta que o container da API vai escutar


# ─────────────────────────────────────────────────────────────────────────────
# ARQUITETURA
# ─────────────────────────────────────────────────────────────────────────────

class Arquitetura:
    def __init__(self):
        # A API do EC2 continua aqui apenas para gerenciar as regras de Rede (Security Groups/VPC)
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
        
        # 1. Cria o RDS primeiro e aguarda para pegar o Endpoint
        self.allocate_rds()
        
        # 2. Aloca bancos NoSQL e Repositórios
        self.allocate_dynamodb()
        self.allocate_s3()
        
        # 3. Cria os contêineres ECS (API + Worker) injetando o Endpoint do RDS
        self.allocate_ecs_services()

        # 4. Conecta no RDS e cria as tabelas vazias (Schema)
        self.create_rds_schema()

    def create_security_groups(self):
        vpcs = self.ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        if not vpcs["Vpcs"]: raise RuntimeError("No default VPC found.")
        vpc_id = vpcs["Vpcs"][0]["VpcId"]
    
        print("[SG]  Configurando Security Groups (Camadas de Rede)...")
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
            
        sgs['alb'] = get_or_create_sg("dijkfood-alb-sg", "SG para o Load Balancer (Aberto para Web)")
        sgs['api'] = get_or_create_sg("dijkfood-api-sg", "SG para os Containers ECS da API")
        sgs['worker'] = get_or_create_sg("dijkfood-worker-sg", "SG para os Containers ECS do Worker")
        sgs['rds'] = get_or_create_sg("dijkfood-rds-sg", "SG para Banco RDS")
    
        # Regras de Ingress
        try: self.ec2.authorize_security_group_ingress(GroupId=sgs['alb'], IpPermissions=[{"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}])
        except ClientError: pass
        try: self.ec2.authorize_security_group_ingress(GroupId=sgs['api'], IpPermissions=[{"IpProtocol": "tcp", "FromPort": API_PORT, "ToPort": API_PORT, "UserIdGroupPairs": [{"GroupId": sgs['alb']}]}])
        except ClientError: pass
        
        # O banco precisa aceitar conexões da API, do Worker e da sua máquina local (para criar o schema)
        try:
            self.ec2.authorize_security_group_ingress(GroupId=sgs['rds'], IpPermissions=[
                {"IpProtocol": "tcp", "FromPort": DB_PORT, "ToPort": DB_PORT, "UserIdGroupPairs": [{"GroupId": sgs['api']}]},
                {"IpProtocol": "tcp", "FromPort": DB_PORT, "ToPort": DB_PORT, "UserIdGroupPairs": [{"GroupId": sgs['worker']}]},
                {"IpProtocol": "tcp", "FromPort": DB_PORT, "ToPort": DB_PORT, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}
            ])
        except ClientError: pass
    
        print(f"[SG]  Security Groups configurados na VPC {vpc_id}")
        return sgs, vpc_id
    
    def create_parameter_group(self):
        try:
            self.rds.create_db_parameter_group(DBParameterGroupName=PG_GROUP_NAME, DBParameterGroupFamily=f"postgres{PG_VERSION}", Description="DijkFood parameter group")
            self.rds.modify_db_parameter_group(DBParameterGroupName=PG_GROUP_NAME, Parameters=[{"ParameterName": "work_mem", "ParameterValue": "4096", "ApplyMethod": "immediate"}])
        except ClientError: pass
        return PG_GROUP_NAME

    def allocate_rds(self):
        print(f"[RDS] Creating '{DB_INSTANCE_ID}' ({INSTANCE_CLASS}) Multi-AZ ...")
        try:
            self.rds.create_db_instance(
                DBInstanceIdentifier=DB_INSTANCE_ID, DBInstanceClass=INSTANCE_CLASS, Engine="postgres",
                EngineVersion=PG_VERSION, MasterUsername=DB_ADMIN_USER, MasterUserPassword=DB_PASSWORD,
                DBName=DB_NAME, AllocatedStorage=20, StorageType="gp2", VpcSecurityGroupIds=[self.sgs['rds']],
                DBParameterGroupName=self.pg_group, PubliclyAccessible=True, BackupRetentionPeriod=1, 
                MultiAZ=True # Requisito: Tolerância a falhas na camada de banco
            )
        except ClientError as e:
            if e.response['Error']['Code'] != 'DBInstanceAlreadyExists': raise

        print(f"[RDS] Aguardando o banco ficar 'Available' para extrair o Endpoint...")
        self.rds.get_waiter("db_instance_available").wait(DBInstanceIdentifier=DB_INSTANCE_ID, WaiterConfig={"Delay": 30, "MaxAttempts": 40})
        self.rds_endpoint = self.rds.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_ID)["DBInstances"][0]["Endpoint"]["Address"]
        print(f"[RDS] Pronto! Endpoint: {self.rds_endpoint}")

    def create_rds_schema(self):
        print(f"\n[RDS] Conectando para criar tabelas vazias (Schema)...")
        conn = None
        for attempt in range(1, 7):
            try:
                conn = psycopg2.connect(host=self.rds_endpoint, port=DB_PORT, dbname=DB_NAME, user=DB_ADMIN_USER, password=DB_PASSWORD, connect_timeout=10)
                break
            except psycopg2.OperationalError as exc:
                if attempt == 6: raise
                print(f"      Tentativa {attempt}/6 falhou. Retentando em 10s...")
                time.sleep(10)
        
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clientes (
                    id_cliente SERIAL PRIMARY KEY,
                    nome VARCHAR(80),
                    email VARCHAR(80),
                    telefone VARCHAR(20),
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION
                );
                CREATE TABLE IF NOT EXISTS restaurantes (
                    id_restaurante SERIAL PRIMARY KEY,
                    nome VARCHAR(80),
                    tipo_cozinha VARCHAR(40),
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION
                );
                CREATE TABLE IF NOT EXISTS entregadores (
                    id_entregador SERIAL PRIMARY KEY,
                    nome VARCHAR(80),
                    tipo_veiculo VARCHAR(30),
                    status VARCHAR(30),
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION
                );
                CREATE TABLE IF NOT EXISTS pedidos (
                    id_pedido SERIAL PRIMARY KEY,
                    id_cliente INT REFERENCES clientes(id_cliente),
                    id_restaurante INT REFERENCES restaurantes(id_restaurante),
                    id_entregador INT REFERENCES entregadores(id_entregador),
                    lista_itens TEXT,
                    data DATE,
                    horario TIME
                );
            """)
        conn.commit()
        conn.close()
        print("[RDS] Tabelas criadas com sucesso (Preparadas para o Simulador)!")

    def allocate_dynamodb(self):
        for tb_name, pk_name in [(DDB_TABLE_EVENTOS, "id_pedido"), (DDB_TABLE_TELEMETRIA, "id_entregador")]:
            print(f"[DDB] Creating table '{tb_name}' ...")
            try:
                table = self.ddb.create_table(
                    TableName=tb_name,
                    KeySchema=[{"AttributeName": pk_name, "KeyType": "HASH"}, {"AttributeName": "timestamp", "KeyType": "RANGE"}],
                    AttributeDefinitions=[{"AttributeName": pk_name, "AttributeType": "S"}, {"AttributeName": "timestamp", "AttributeType": "S"}],
                    BillingMode="PAY_PER_REQUEST"
                )
                table.wait_until_exists()
            except ClientError: pass

    def allocate_s3(self):
        print(f"[S3]  Creating bucket '{S3_BUCKET_NAME}' ...")
        try:
            self.s3.create_bucket(Bucket=S3_BUCKET_NAME)
        except ClientError: pass

    def allocate_ecs_services(self):
        print(f"\n--- Criando Cluster ECS e Serviços ---")
        subnets = [s['SubnetId'] for s in self.ec2.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [self.vpc_id]}])['Subnets']]

        # 1. Cluster
        self.ecs.create_cluster(clusterName=ECS_CLUSTER_NAME)
        print(f"[ECS] Cluster '{ECS_CLUSTER_NAME}' verificado/criado.")

        # ==========================================
        # CONFIGURAÇÃO DO SERVIÇO DA API (Com ALB)
        # ==========================================
        print(f"\n[ECS-API] Configurando Camada de API...")
        alb_arn = None
        try:
            alb = self.elbv2.create_load_balancer(Name=ALB_NAME, Subnets=subnets, SecurityGroups=[self.sgs['alb']], Scheme='internet-facing')
            alb_arn = alb['LoadBalancers'][0]['LoadBalancerArn']
        except ClientError as e:
            if e.response['Error']['Code'] == 'DuplicateLoadBalancerName':
                alb_arn = self.elbv2.describe_load_balancers(Names=[ALB_NAME])['LoadBalancers'][0]['LoadBalancerArn']
            else: raise

        tg_arn = None
        try:
            tg = self.elbv2.create_target_group(Name=TG_NAME, Protocol='HTTP', Port=API_PORT, VpcId=self.vpc_id, TargetType='ip', HealthCheckPath='/')
            tg_arn = tg['TargetGroups'][0]['TargetGroupArn']
        except ClientError as e:
            if e.response['Error']['Code'] == 'DuplicateTargetGroupName':
                tg_arn = self.elbv2.describe_target_groups(Names=[TG_NAME])['TargetGroups'][0]['TargetGroupArn']
            else: raise

        listeners = self.elbv2.describe_listeners(LoadBalancerArn=alb_arn)['Listeners']
        if not any(l['Port'] == 80 for l in listeners):
            self.elbv2.create_listener(LoadBalancerArn=alb_arn, Protocol='HTTP', Port=80, DefaultActions=[{'Type': 'forward', 'TargetGroupArn': tg_arn}])

        api_task_arn = None
        try:
            response = self.ecs.register_task_definition(
                family="dijkfood-api-task", networkMode="awsvpc", executionRoleArn=LAB_ROLE_ARN, taskRoleArn=LAB_ROLE_ARN,
                requiresCompatibilities=["FARGATE"], cpu="256", memory="512",
                containerDefinitions=[{
                    "name": "dijkfood-api-container",
                    "image": "nginxdemos/hello", # Substitua pela sua imagem Docker da API
                    "portMappings": [{"containerPort": API_PORT, "hostPort": API_PORT, "protocol": "tcp"}],
                    "environment": [
                        {"name": "DB_HOST", "value": self.rds_endpoint},
                        {"name": "DB_USER", "value": DB_ADMIN_USER},
                        {"name": "DB_PASS", "value": DB_PASSWORD},
                        {"name": "DB_NAME", "value": DB_NAME},
                        {"name": "DDB_EVENTOS", "value": DDB_TABLE_EVENTOS},
                        {"name": "DDB_TELEMETRIA", "value": DDB_TABLE_TELEMETRIA}
                    ]
                }]
            )
            api_task_arn = response['taskDefinition']['taskDefinitionArn']
        except ClientError as e: print(f"[ECS-API] Aviso Task Def: {e}")

        try:
            self.ecs.create_service(
                cluster=ECS_CLUSTER_NAME, serviceName="dijkfood-api-service", taskDefinition=api_task_arn,
                desiredCount=2, launchType="FARGATE",
                networkConfiguration={'awsvpcConfiguration': {'subnets': subnets, 'securityGroups': [self.sgs['api']], 'assignPublicIp': 'ENABLED'}},
                loadBalancers=[{'targetGroupArn': tg_arn, 'containerName': 'dijkfood-api-container', 'containerPort': API_PORT}]
            )
            print(f"[ECS-API] Serviço Fargate iniciado!")
        except ClientError as e:
            if e.response['Error']['Code'] != 'InvalidParameterException': print(f"[ECS-API] Aviso Service: {e}")

        # Auto-Scaling da API
        resource_id = f"service/{ECS_CLUSTER_NAME}/dijkfood-api-service"
        try:
            self.app_asg.register_scalable_target(
                ServiceNamespace='ecs', 
                ResourceId=resource_id, 
                ScalableDimension='ecs:service:DesiredCount', 
                MinCapacity=2, 
                MaxCapacity=10
            )
            self.app_asg.put_scaling_policy(
                PolicyName='dijkfood-api-cpu-scaling',
                ServiceNamespace='ecs', 
                ResourceId=resource_id, 
                ScalableDimension='ecs:service:DesiredCount',
                PolicyType='TargetTrackingScaling',
                TargetTrackingScalingPolicyConfiguration={
                    'TargetValue': 70.0, 
                    'PredefinedMetricSpecification': {
                        'PredefinedMetricType': 'ECSServiceAverageCPUUtilization'
                    }, 
                    'ScaleOutCooldown': 60, 
                    'ScaleInCooldown': 60}
            )
        except ClientError as e: print(f"[ECS-API] Aviso Auto-scaling: {e}")


        # ==========================================
        # CONFIGURAÇÃO DO SERVIÇO DO WORKER (Sem ALB)
        # ==========================================
        print(f"\n[ECS-Worker] Configurando Camada de Cálculo de Rotas...")
        worker_task_arn = None
        try:
            response = self.ecs.register_task_definition(
                family="dijkfood-worker-task", networkMode="awsvpc", executionRoleArn=LAB_ROLE_ARN, taskRoleArn=LAB_ROLE_ARN,
                requiresCompatibilities=["FARGATE"], 
                cpu="512", memory="1024", # Mais RAM para armazenar o grafo viário de SP
                containerDefinitions=[{
                    "name": "dijkfood-worker-container",
                    "image": "python:3.9-slim", # Substitua pela sua imagem Docker do Script de Worker
                    "environment": [
                        {"name": "DB_HOST", "value": self.rds_endpoint},
                        {"name": "DB_USER", "value": DB_ADMIN_USER},
                        {"name": "DB_PASS", "value": DB_PASSWORD},
                        {"name": "DB_NAME", "value": DB_NAME},
                        {"name": "S3_BUCKET", "value": S3_BUCKET_NAME} # Necessário para o script baixar o grafo
                    ]
                }]
            )
            worker_task_arn = response['taskDefinition']['taskDefinitionArn']
        except ClientError as e: print(f"[ECS-Worker] Aviso Task Def: {e}")

        try:
            self.ecs.create_service(
                cluster=ECS_CLUSTER_NAME, serviceName="dijkfood-worker-service", taskDefinition=worker_task_arn,
                desiredCount=2, # Requisito: Alta Disponibilidade e Tolerância a Falhas
                launchType="FARGATE",
                networkConfiguration={'awsvpcConfiguration': {'subnets': subnets, 'securityGroups': [self.sgs['worker']], 'assignPublicIp': 'ENABLED'}}
            )
            print(f"[ECS-Worker] Serviço de processamento em background iniciado com 2 workers!")
        except ClientError as e:
            if e.response['Error']['Code'] != 'InvalidParameterException': print(f"[ECS-Worker] Aviso Service: {e}")

        print(f"\n[ALB] URL da sua API: http://{self.elbv2.describe_load_balancers(Names=[ALB_NAME])['LoadBalancers'][0]['DNSName']}")
        # ==========================================
        # AUTO-SCALING DO SERVIÇO DO WORKER
        # ==========================================
        print(f"\n[ECS-Worker] Configurando Auto-Scaling (Target 70% CPU)...")
        worker_resource_id = f"service/{ECS_CLUSTER_NAME}/dijkfood-worker-service"
        try:
            # 1. Registar o Worker como um alvo escalável (Mínimo 2, Máximo 10 contentores)
            self.app_asg.register_scalable_target(
                ServiceNamespace='ecs', 
                ResourceId=worker_resource_id, 
                ScalableDimension='ecs:service:DesiredCount', 
                MinCapacity=2, 
                MaxCapacity=10
            )
            
            # 2. Aplicar a política de escalamento baseada na CPU
            self.app_asg.put_scaling_policy(
                PolicyName='dijkfood-worker-cpu-scaling', 
                ServiceNamespace='ecs', 
                ResourceId=worker_resource_id, 
                ScalableDimension='ecs:service:DesiredCount',
                PolicyType='TargetTrackingScaling',
                TargetTrackingScalingPolicyConfiguration={
                    'TargetValue': 70.0, 
                    'PredefinedMetricSpecification': {
                        'PredefinedMetricType': 'ECSServiceAverageCPUUtilization'
                    },
                    'ScaleOutCooldown': 60,  # Tempo de espera antes de adicionar mais
                    'ScaleInCooldown': 60    # Tempo de espera antes de remover
                }
            )
            print(f"[ECS-Worker] Auto-Scaling ativado com sucesso!")
        except ClientError as e: 
            print(f"[ECS-Worker] Aviso Auto-scaling: {e}")

    # ─────────────────────────────────────────────────────────────────────────────
    # POPULATE (Apenas o S3, o Simulador assume o resto)
    # ─────────────────────────────────────────────────────────────────────────────

    def populate_graph_s3(self):
        print(f"\n--- Baixando Grafo OSMnx e enviando para S3 ---")
        
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
        self.s3.upload_file(edges_file, S3_BUCKET_NAME, edges_file)
        self.s3.upload_file(nodes_file, S3_BUCKET_NAME, nodes_file)
        print("[S3] Upload concluído!")

    # ─────────────────────────────────────────────────────────────────────────────
    # TEARDOWN (Limpeza Segura de Custos)
    # ─────────────────────────────────────────────────────────────────────────────

    def destroy(self):
        print("\n── Teardown " + "─" * 55)

        print(f"[ECS] Deletando ECS Services e Load Balancer...")
        for svc in ["dijkfood-api-service", "dijkfood-worker-service"]:
            try:
                self.ecs.update_service(cluster=ECS_CLUSTER_NAME, service=svc, desiredCount=0)
            except ClientError: pass
        
        time.sleep(10) # Espera os containers desligarem
        
        for svc in ["dijkfood-api-service", "dijkfood-worker-service"]:
            try: self.ecs.delete_service(cluster=ECS_CLUSTER_NAME, service=svc)
            except ClientError: pass
    
        try:
            tg_arn = self.elbv2.describe_target_groups(Names=[TG_NAME])['TargetGroups'][0]['TargetGroupArn']
            alb_arn = self.elbv2.describe_load_balancers(Names=[ALB_NAME])['LoadBalancers'][0]['LoadBalancerArn']
            self.elbv2.delete_load_balancer(LoadBalancerArn=alb_arn)
            time.sleep(15) # Espera o ALB ser deletado
            self.elbv2.delete_target_group(TargetGroupArn=tg_arn)
            print("[ALB] Load Balancer deletado.")
        except ClientError: pass
    
        try: self.ecs.delete_cluster(cluster=ECS_CLUSTER_NAME)
        except ClientError: pass
    
        # 3. S3
        print(f"[S3]  Emptying and deleting bucket '{S3_BUCKET_NAME}' ...")
        try:
            response = self.s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
            if 'Contents' in response:
                objects = [{'Key': obj['Key']} for obj in response['Contents']]
                self.s3.delete_objects(Bucket=S3_BUCKET_NAME, Delete={'Objects': objects})
            self.s3.delete_bucket(Bucket=S3_BUCKET_NAME)
        except ClientError: pass
    
        # 4. DynamoDB 
        for tb_name in [DDB_TABLE_EVENTOS, DDB_TABLE_TELEMETRIA]:
            print(f"[DDB] Deleting table '{tb_name}' ...")
            try: self.ddb.Table(tb_name).delete()
            except ClientError: pass
    
        # 5. RDS & Security Groups
        print(f"[RDS] Deleting primary '{DB_INSTANCE_ID}' ...")
        try:
            self.rds.delete_db_instance(DBInstanceIdentifier=DB_INSTANCE_ID, SkipFinalSnapshot=True, DeleteAutomatedBackups=True)
            self.rds.get_waiter("db_instance_deleted").wait(DBInstanceIdentifier=DB_INSTANCE_ID, WaiterConfig={"Delay": 30, "MaxAttempts": 40})
        except ClientError: pass
    
        try: self.rds.delete_db_parameter_group(DBParameterGroupName=PG_GROUP_NAME)
        except ClientError: pass
    
        # Deletar SGs no final (Após serviços do ECS e RDS estarem mortos)
        for sg_name in ["dijkfood-api-sg", "dijkfood-worker-sg", "dijkfood-alb-sg", "dijkfood-rds-sg"]:
            try:
                sgs = self.ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [sg_name]}])
                if sgs["SecurityGroups"]: 
                    self.ec2.delete_security_group(GroupId=sgs["SecurityGroups"][0]["GroupId"])
            except ClientError: pass

# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="DijkFood Full Architecture Deployment")
    parser.add_argument("--step", choices=["all", "allocate", "populate", "destroy"], default="all")
    args = parser.parse_args()

    arquitetura = Arquitetura()

    if args.step in ("all", "allocate"):
        arquitetura.allocate()

    if args.step in ("all", "populate"):
        arquitetura.populate_graph_s3()

    if args.step in ("all", "destroy"):
        arquitetura.destroy()

if __name__ == "__main__":
    main()
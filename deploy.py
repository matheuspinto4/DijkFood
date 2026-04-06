"""
deploy.py — Full Architecture Deployment (RDS Multi-AZ, DDB, S3, EC2 HA, ECS Auto-Scaled)
Criado para o projeto DijkFood

Restrito aos recursos exigidos: EC2, RDS, DynamoDB, ECS, S3.

Lifecycle:
  1. allocate   — Cria toda a infraestrutura e as tabelas vazias no Banco de Dados
  2. populate   — Baixa o Grafo Viário (OSMnx) e envia para o S3
  3. destroy    — Apaga rigorosamente TODOS os recursos

Usage:
  python deploy.py                           # Cria, popula o S3 e APAGA em seguida
  python deploy.py --step allocate           # Cria a infraestrutura e o schema vazio
  python deploy.py --step populate           # Envia o Grafo para o S3
  python deploy.py --step destroy            # Apaga toda a infraestrutura
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

# Configurações S3 & Grafo
S3_BUCKET_NAME = f"dijkfood-grafo-sp-{ACCOUNT_ID}"
PLACE_NAME     = "São Paulo, Brazil"
NETWORK_TYPE   = "drive"

# Configurações EC2
EC2_INSTANCE_TYPE = "t3.micro"
KEY_PAIR_NAME     = "vockey"

# Configurações ECS
ECS_CLUSTER_NAME = "DijkFoodCluster"
ALB_NAME         = "dijkfood-api-alb"
TG_NAME          = "dijkfood-api-tg"
API_PORT         = 80 # A porta que o container vai escutar


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
        
        # 1. Cria o RDS primeiro e aguarda para pegar o Endpoint (Necessário para o ECS)
        self.allocate_rds()
        
        # 2. Aloca os recursos independentes
        self.allocate_ec2()
        self.allocate_dynamodb()
        self.allocate_s3()
        
        # 3. Cria o ECS injetando o Endpoint do RDS e nomes das tabelas do DDB
        self.allocate_ecs_and_alb()

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
        sgs['ecs'] = get_or_create_sg("dijkfood-ecs-sg", "SG para os Containers ECS (Recebe do ALB)")
        sgs['ec2'] = get_or_create_sg("dijkfood-worker-sg", "SG para EC2 Worker (Dijkstra)")
        sgs['rds'] = get_or_create_sg("dijkfood-rds-sg", "SG para Banco RDS")
    
        try: self.ec2.authorize_security_group_ingress(GroupId=sgs['alb'], IpPermissions=[{"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}])
        except ClientError: pass
        try: self.ec2.authorize_security_group_ingress(GroupId=sgs['ecs'], IpPermissions=[{"IpProtocol": "tcp", "FromPort": API_PORT, "ToPort": API_PORT, "UserIdGroupPairs": [{"GroupId": sgs['alb']}]}])
        except ClientError: pass
        try: self.ec2.authorize_security_group_ingress(GroupId=sgs['ec2'], IpPermissions=[{"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}])
        except ClientError: pass
        try:
            self.ec2.authorize_security_group_ingress(GroupId=sgs['rds'], IpPermissions=[
                {"IpProtocol": "tcp", "FromPort": DB_PORT, "ToPort": DB_PORT, "UserIdGroupPairs": [{"GroupId": sgs['ecs']}]},
                {"IpProtocol": "tcp", "FromPort": DB_PORT, "ToPort": DB_PORT, "UserIdGroupPairs": [{"GroupId": sgs['ec2']}]},
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
    
    def allocate_ec2(self):
        ssm = boto3.client("ssm", region_name=REGION)
        ami_id = ssm.get_parameter(Name="/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2")["Parameter"]["Value"]
        print(f"[EC2] Creating 2 Worker Nodes ({EC2_INSTANCE_TYPE}) ...")
        try:
            # Requisito: 2 Máquinas para demonstrar tolerância a falha (encerrando uma ao vivo)
            self.ec2.run_instances(
                ImageId=ami_id, InstanceType=EC2_INSTANCE_TYPE, KeyName=KEY_PAIR_NAME, 
                SecurityGroupIds=[self.sgs['ec2']], MinCount=2, MaxCount=2,
                TagSpecifications=[{"ResourceType": "instance", "Tags": [{"Key": "Name", "Value": "DijkFood-Worker"}]}]
            )
        except ClientError as e: print(f"[EC2] Warning: {e}")

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

    def allocate_ecs_and_alb(self):
        print(f"\n--- Criando Camada de API REST (ECS + Load Balancer) ---")
        subnets = [s['SubnetId'] for s in self.ec2.describe_subnets(Filters=[{'Name': 'vpc-id', 'Values': [self.vpc_id]}])['Subnets']]

        # 1. Cluster
        self.ecs.create_cluster(clusterName=ECS_CLUSTER_NAME)
        print(f"[ECS] Cluster '{ECS_CLUSTER_NAME}' verificado/criado.")

        # 2. Load Balancer
        alb_arn = None
        try:
            alb = self.elbv2.create_load_balancer(Name=ALB_NAME, Subnets=subnets, SecurityGroups=[self.sgs['alb']], Scheme='internet-facing')
            alb_arn = alb['LoadBalancers'][0]['LoadBalancerArn']
            print(f"[ALB] Load Balancer '{ALB_NAME}' criado.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'DuplicateLoadBalancerName':
                alb_arn = self.elbv2.describe_load_balancers(Names=[ALB_NAME])['LoadBalancers'][0]['LoadBalancerArn']
            else: raise

        # 3. Target Group
        tg_arn = None
        try:
            tg = self.elbv2.create_target_group(Name=TG_NAME, Protocol='HTTP', Port=API_PORT, VpcId=self.vpc_id, TargetType='ip', HealthCheckPath='/')
            tg_arn = tg['TargetGroups'][0]['TargetGroupArn']
        except ClientError as e:
            if e.response['Error']['Code'] == 'DuplicateTargetGroupName':
                tg_arn = self.elbv2.describe_target_groups(Names=[TG_NAME])['TargetGroups'][0]['TargetGroupArn']
            else: raise

        # 4. Listener
        listeners = self.elbv2.describe_listeners(LoadBalancerArn=alb_arn)['Listeners']
        if not any(l['Port'] == 80 for l in listeners):
            self.elbv2.create_listener(LoadBalancerArn=alb_arn, Protocol='HTTP', Port=80, DefaultActions=[{'Type': 'forward', 'TargetGroupArn': tg_arn}])

        # 5. Task Definition (Com variáveis de ambiente injetadas!)
        task_def_arn = None
        try:
            response = self.ecs.register_task_definition(
                family="dijkfood-api-task",
                networkMode="awsvpc",
                executionRoleArn=LAB_ROLE_ARN,
                taskRoleArn=LAB_ROLE_ARN,
                requiresCompatibilities=["FARGATE"],
                cpu="256", memory="512",
                containerDefinitions=[{
                    "name": "dijkfood-api-container",
                    "image": "nginxdemos/hello", # Substitua por sua imagem Docker real
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
            task_def_arn = response['taskDefinition']['taskDefinitionArn']
            print(f"[ECS] Task Definition registrada com variáveis de ambiente do RDS e DynamoDB.")
        except ClientError as e: print(f"[ECS] Aviso Task Def: {e}")

        # 6. Service Fargate
        try:
            self.ecs.create_service(
                cluster=ECS_CLUSTER_NAME, serviceName="dijkfood-api-service", taskDefinition=task_def_arn,
                desiredCount=2, launchType="FARGATE",
                networkConfiguration={'awsvpcConfiguration': {'subnets': subnets, 'securityGroups': [self.sgs['ecs']], 'assignPublicIp': 'ENABLED'}},
                loadBalancers=[{'targetGroupArn': tg_arn, 'containerName': 'dijkfood-api-container', 'containerPort': API_PORT}]
            )
            print(f"[ECS] Serviço Fargate iniciado com 2 contêineres!")
        except ClientError as e:
            if e.response['Error']['Code'] != 'InvalidParameterException': print(f"[ECS] Aviso Service: {e}")

        # 7. AUTO-SCALING (Requisito Crítico do Simulador)
        print(f"[ECS] Configurando Auto-Scaling (Target 70% CPU)...")
        resource_id = f"service/{ECS_CLUSTER_NAME}/dijkfood-api-service"
        try:
            self.app_asg.register_scalable_target(
                ServiceNamespace='ecs', ResourceId=resource_id, ScalableDimension='ecs:service:DesiredCount', MinCapacity=2, MaxCapacity=10
            )
            self.app_asg.put_scaling_policy(
                PolicyName='dijkfood-api-cpu-scaling', ServiceNamespace='ecs', ResourceId=resource_id, ScalableDimension='ecs:service:DesiredCount',
                PolicyType='TargetTrackingScaling',
                TargetTrackingScalingPolicyConfiguration={
                    'TargetValue': 70.0, 'PredefinedMetricSpecification': {'PredefinedMetricType': 'ECSServiceAverageCPUUtilization'},
                    'ScaleOutCooldown': 60, 'ScaleInCooldown': 60
                }
            )
        except ClientError as e: print(f"[ECS] Aviso Auto-scaling: {e}")

        print(f"\n[ALB] URL da sua API: http://{self.elbv2.describe_load_balancers(Names=[ALB_NAME])['LoadBalancers'][0]['DNSName']}")

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
            print("Passei do for")

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

        # 1. Destruir ECS e ALB (A ordem importa!)
        print(f"[ECS] Deletando ECS Service e Load Balancer...")
        try:
            self.ecs.update_service(cluster=ECS_CLUSTER_NAME, service="dijkfood-api-service", desiredCount=0)
            time.sleep(10) # Espera os containers desligarem
            self.ecs.delete_service(cluster=ECS_CLUSTER_NAME, service="dijkfood-api-service")
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
    
        # 2. EC2
        print(f"[EC2] Terminating instances with name 'DijkFood-Worker' ...")
        instances = self.ec2.describe_instances(Filters=[{"Name": "tag:Name", "Values": ["DijkFood-Worker"]}])
        for res in instances.get("Reservations", []):
            for inst in res.get("Instances", []):
                if inst["State"]["Name"] != "terminated":
                    self.ec2.terminate_instances(InstanceIds=[inst["InstanceId"]])
    
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
    
        # Deletar SGs no final (Após RDS e EC2 estarem mortos)
        for sg_name in ["dijkfood-ecs-sg", "dijkfood-alb-sg", "dijkfood-rds-sg", "dijkfood-worker-sg"]:
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

    asyncio.run(simulator())

    if args.step in ("all", "destroy"):
        arquitetura.destroy()

if __name__ == "__main__":
    main()
import os
import boto3
import time
import uuid
import csv
import osmnx as ox
import psycopg2
import psycopg2.extras
import random
from botocore.exceptions import ClientError

from seed_relational_database import seed_relational_database

from faker import Faker
from faker.providers import BaseProvider
import random

class RestauranteProvider(BaseProvider):
    tipos_culinaria = [
        'Pizzaria', 'Churrascaria', 'Comida Japonesa', 'Comida Mineira', 
        'Comida Baiana', 'Lanchonete', 'Hamburgueria', 'Cantina Italiana', 
        'Comida Vegana', 'Bistrô Francês', 'Frutos do Mar'
    ]

    prefixos = ['Restaurante', 'Cantina', 'Pizzaria', 'Bar e Petiscaria', 'Recanto', 'Espaço']
    sufixos = ['Gourmet', 'da Família', 'Tradicional', 'Express', 'Saboroso', 'Grill']

    def tipo_restaurante(self):
        """Retorna uma categoria de restaurante aleatória."""
        return self.random_element(self.tipos_culinaria)

    def nome_restaurante(self):
        """Gera um nome de restaurante combinando palavras ou usando nomes de pessoas."""
        formato = random.choice(['nome_pessoa', 'prefixo_sufixo', 'sobrenome'])
        
        if formato == 'nome_pessoa':
            # Ex: Bar do João, Restaurante da Maria
            artigo = self.random_element(['do', 'da'])
            nome = self.generator.first_name()
            estabelecimento = self.random_element(['Bar', 'Restaurante', 'Cantina', 'Lanchonete'])
            return f"{estabelecimento} {artigo} {nome}"
            
        elif formato == 'prefixo_sufixo':
            # Ex: Restaurante Gourmet, Recanto Tradicional
            return f"{self.random_element(self.prefixos)} {self.random_element(self.sufixos)}"
            
        else:
            # Ex: Cantina Silva, Pizzaria Oliveira
            estabelecimento = self.random_element(self.prefixos)
            sobrenome = self.generator.last_name()
            return f"{estabelecimento} {sobrenome}"


def get_lat_lon(): return random.uniform(-23.7, -23.4), random.uniform(-46.8, -46.3)

def gerar_dados_falsos(numero_de_clientes : int, numero_de_restaurantes : int, numero_de_entregadores : int):
    """
    Gera dados falsos de clientes, restaurantes e entregadores para popular o RDS.
    """
    fake = Faker(['pt-BR'])
    fake.add_provider(RestauranteProvider)

    
    clientes = [(f"{fake.name()}", f"{fake.email()}", f"{fake.phone_number()}", *get_lat_lon()) for i in range(numero_de_clientes)]
    restaurantes = [(f"{fake.nome_restaurante()}", f"{fake.tipo_restaurante()}", *get_lat_lon()) for i in range(numero_de_restaurantes)]
    entregadores = [(f"{fake.name()}", f"{random.choice(["moto", "carro", "caminhao", "biscicleta", "pé", "cavalo", "triciclo", "chihuahua", "galinha"])}", False, *get_lat_lon()) for i in range(numero_de_entregadores)]

    return (clientes, restaurantes, entregadores)


class DijkFoodDeployer:
    def __init__(self, region="us-east-1"):
        self.region = region
        self.ec2 = boto3.client('ec2', region_name=region)
        self.s3 = boto3.client('s3', region_name=region)
        self.rds = boto3.client('rds', region_name=region)
        self.dynamodb = boto3.client('dynamodb', region_name=region)
        self.ecs = boto3.client('ecs', region_name=region)
        self.elbv2 = boto3.client('elbv2', region_name=region)
        self.sts = boto3.client('sts')
        self.resource_ids = {}
        
        # Identifica a conta atual e a LabRole obrigatória do AWS Academy
        self.account_id = self.sts.get_caller_identity()['Account']
        self.lab_role_arn = f"arn:aws:iam::{self.account_id}:role/LabRole"
        print(f"Iniciando deploy na conta: {self.account_id} usando LabRole.")

        # Configurações do Banco de Dados Relacional
        self.db_name = 'postgres'
        self.db_user = 'dijkadmin'
        self.db_pass = 'DijkFoodPassword123!'
        self.db_endpoint = None

    def create_network_and_security(self):
        print("\n--- 1. Criando Rede e Segurança ---")
        # 1.1 VPC
        vpc = self.ec2.create_vpc(CidrBlock='10.0.0.0/16')
        vpc_id = vpc['Vpc']['VpcId']
        self.resource_ids['VpcId'] = vpc_id
        self.ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={'Value': True})
        self.ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={'Value': True})
        
        # 1.2 Internet Gateway e Roteamento
        igw = self.ec2.create_internet_gateway()
        igw_id = igw['InternetGateway']['InternetGatewayId']
        self.ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        
        rt = self.ec2.create_route_table(VpcId=vpc_id)
        rt_id = rt['RouteTable']['RouteTableId']
        self.ec2.create_route(RouteTableId=rt_id, DestinationCidrBlock='0.0.0.0/0', GatewayId=igw_id)

        # 1.3 Subnets (Habilitando IP Público para contornar a falta de NAT Gateway no Lab)
        sub1 = self.ec2.create_subnet(VpcId=vpc_id, CidrBlock='10.0.1.0/24', AvailabilityZone=f"{self.region}a")
        sub2 = self.ec2.create_subnet(VpcId=vpc_id, CidrBlock='10.0.2.0/24', AvailabilityZone=f"{self.region}b")
        self.resource_ids['Subnets'] = [sub1['Subnet']['SubnetId'], sub2['Subnet']['SubnetId']]
        
        for sub_id in self.resource_ids['Subnets']:
            self.ec2.associate_route_table(SubnetId=sub_id, RouteTableId=rt_id)
            self.ec2.modify_subnet_attribute(SubnetId=sub_id, MapPublicIpOnLaunch={'Value': True})

        # 1.4 Security Groups (A Mágica da Segurança)
        sg_alb = self.ec2.create_security_group(GroupName='ALB-SG', Description='ALB SG', VpcId=vpc_id)
        self.resource_ids['SgAlb'] = sg_alb['GroupId']
        self.ec2.authorize_security_group_ingress(
            GroupId=self.resource_ids['SgAlb'],
            IpPermissions=[{'IpProtocol': 'tcp', 'FromPort': 80, 'ToPort': 80, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}]
        )

        sg_ecs = self.ec2.create_security_group(GroupName='ECS-SG', Description='ECS SG', VpcId=vpc_id)
        self.resource_ids['SgEcs'] = sg_ecs['GroupId']
        self.ec2.authorize_security_group_ingress(
            GroupId=self.resource_ids['SgEcs'],
            IpPermissions=[{'IpProtocol': 'tcp', 'FromPort': 8000, 'ToPort': 8000, 'UserIdGroupPairs': [{'GroupId': self.resource_ids['SgAlb']}]}]
        )

        sg_rds = self.ec2.create_security_group(GroupName='RDS-SG', Description='RDS SG', VpcId=vpc_id)
        self.resource_ids['SgRds'] = sg_rds['GroupId']
        self.ec2.authorize_security_group_ingress(
            GroupId=self.resource_ids['SgRds'],
            IpPermissions=[{'IpProtocol': 'tcp', 'FromPort': 5432, 'ToPort': 5432, 'UserIdGroupPairs': [{'GroupId': self.resource_ids['SgEcs']}]}]
        )
        print("Rede e Security Groups criados com sucesso.")

    def create_storage_and_nosql(self):
        print("\n--- 2. Criando S3 e DynamoDB ---")
        
        # S3 (Verificando se já existe)
        bucket_name = f"dijkfood-grafo-sp-projeto" 
        try:
            self.s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket S3 criado: {bucket_name}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['BucketAlreadyExists', 'BucketAlreadyOwnedByYou']:
                print(f"Bucket S3 '{bucket_name}' já existe. Pulando criação...")
            else:
                raise e 
        
        self.resource_ids['S3Bucket'] = bucket_name

        # DynamoDB (Verificando se as tabelas já existem)
        tables = [
            {'name': 'Historico_Eventos', 'pk': 'PedidoID'},
            {'name': 'Telemetria_Entregadores', 'pk': 'EntregadorID'}
        ]
        
        for t in tables:
            try:
                self.dynamodb.create_table(
                    TableName=t['name'],
                    KeySchema=[
                        {'AttributeName': t['pk'], 'KeyType': 'HASH'},
                        {'AttributeName': 'Timestamp', 'KeyType': 'RANGE'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': t['pk'], 'AttributeType': 'S'},
                        {'AttributeName': 'Timestamp', 'AttributeType': 'N'}
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                print(f"Tabela DynamoDB criada: {t['name']}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceInUseException':
                    print(f"Tabela DynamoDB '{t['name']}' já existe. Pulando criação...")
                else:
                    raise e

    def prepare_and_upload_graph(self):
        print("\n--- 3. Preparando Grafo Viário (OSMnx) ---")
        place = "São Paulo, Brazil"
        bucket_name = self.resource_ids.get('S3Bucket')
        
        if not bucket_name:
            raise ValueError("Bucket S3 não encontrado. Execute a criação do storage primeiro.")

        edges_file = "edges.csv"
        nodes_file = "nodes.csv"

        # Verificador de Cache Local
        if os.path.exists(edges_file) and os.path.exists(nodes_file):
            print(f"[Grafo] Arquivos locais '{edges_file}' e '{nodes_file}' encontrados!")
            print("[Grafo] Pulando o download do OSMnx para economizar tempo.")
        else:
            # 1. Download do Grafo (Só executa se os arquivos não existirem)
            print(f"[Grafo] Baixando a rede 'drive' para '{place}' (Pode levar alguns minutos)...")
            graph = ox.graph_from_place(place, network_type="drive")
            print(f"[Grafo] Download concluído: {graph.number_of_nodes()} nós e {graph.number_of_edges()} arestas.")

            # 2. Processamento de Arestas (Edges)
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
            print(f"[Grafo] Arquivo local {edges_file} gerado.")

            # 3. Processamento de Nós (Nodes)
            with open(nodes_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["node_id", "lat", "lon"])
                for n, data in graph.nodes(data=True):
                    writer.writerow([n, data['y'], data['x']])
            print(f"[Grafo] Arquivo local {nodes_file} gerado (com Lat/Lon para a API).")

        # 4. Upload Automático para o S3 (Sempre executa, garantindo que o bucket tenha os arquivos)
        print(f"[S3] Fazendo upload dos arquivos CSV para o bucket '{bucket_name}'...")
        self.s3.upload_file(edges_file, bucket_name, edges_file)
        self.s3.upload_file(nodes_file, bucket_name, nodes_file)
        print("[S3] Upload concluído! Os contêineres já podem baixar o grafo na inicialização.")

    def create_rds_database(self):
        print("\n--- 4. Criando RDS (PostgreSQL) ---")
        
        try:
            self.rds.create_db_subnet_group(
                DBSubnetGroupName='dijkfood-subnet-group',
                DBSubnetGroupDescription='Subnet group for DijkFood RDS',
                SubnetIds=self.resource_ids['Subnets']
            )
            print("DB Subnet Group 'dijkfood-subnet-group' criado.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'DBSubnetGroupAlreadyExists':
                print("DB Subnet Group 'dijkfood-subnet-group' já existe. Pulando criação...")
            else:
                raise e

        try:
            self.rds.create_db_instance(
                DBInstanceIdentifier='dijkfood-db',
                Engine='postgres',
                DBInstanceClass='db.t3.micro', 
                AllocatedStorage=20,
                MasterUsername=self.db_user,
                MasterUserPassword=self.db_pass,
                VpcSecurityGroupIds=[self.resource_ids['SgRds']],
                DBSubnetGroupName='dijkfood-subnet-group',
                PubliclyAccessible=True, # Necessário para o script local injetar os dados
                MultiAZ=False
            )
            print("Instância RDS solicitada.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'DBInstanceAlreadyExists':
                print("Instância RDS 'dijkfood-db' já existe. Pulando criação...")
            else:
                raise e

        # Waiter: Pausa o script até a AWS ligar a máquina do banco
        print("Aguardando o RDS ficar disponível (Isso pode levar de 5 a 10 minutos)...")
        waiter = self.rds.get_waiter('db_instance_available')
        waiter.wait(DBInstanceIdentifier='dijkfood-db', WaiterConfig={'Delay': 30, 'MaxAttempts': 40})
        
        info = self.rds.describe_db_instances(DBInstanceIdentifier='dijkfood-db')
        self.db_endpoint = info['DBInstances'][0]['Endpoint']['Address']
        print(f"RDS Disponível! Endpoint: {self.db_endpoint}")

    def create_compute_layer(self):
        print("\n--- 6. Criando Cluster ECS e Load Balancer ---")
        
        self.ecs.create_cluster(clusterName='DijkFoodCluster')
        print("Cluster ECS 'DijkFoodCluster' verificado/criado.")
        
        try:
            alb = self.elbv2.create_load_balancer(
                Name='dijkfood-alb',
                Subnets=self.resource_ids['Subnets'],
                SecurityGroups=[self.resource_ids['SgAlb']],
                Scheme='internet-facing'
            )
            alb_arn = alb['LoadBalancers'][0]['LoadBalancerArn']
            print("Load Balancer 'dijkfood-alb' criado.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'DuplicateLoadBalancerName':
                print("Load Balancer 'dijkfood-alb' já existe. Recuperando ARN...")
                albs = self.elbv2.describe_load_balancers(Names=['dijkfood-alb'])
                alb_arn = albs['LoadBalancers'][0]['LoadBalancerArn']
            else:
                raise e
        
        self.resource_ids['AlbArn'] = alb_arn

        try:
            tg = self.elbv2.create_target_group(
                Name='dijkfood-tg',
                Protocol='HTTP',
                Port=8000,
                VpcId=self.resource_ids['VpcId'],
                TargetType='ip',
                HealthCheckPath='/health'
            )
            tg_arn = tg['TargetGroups'][0]['TargetGroupArn']
            print("Target Group 'dijkfood-tg' criado.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'DuplicateTargetGroupName':
                print("Target Group 'dijkfood-tg' já existe. Recuperando ARN...")
                tgs = self.elbv2.describe_target_groups(Names=['dijkfood-tg'])
                tg_arn = tgs['TargetGroups'][0]['TargetGroupArn']
            else:
                raise e

        listeners = self.elbv2.describe_listeners(LoadBalancerArn=alb_arn)['Listeners']
        porta_80_em_uso = any(l['Port'] == 80 for l in listeners)
        
        if not porta_80_em_uso:
            self.elbv2.create_listener(
                LoadBalancerArn=alb_arn,
                Protocol='HTTP',
                Port=80,
                DefaultActions=[{'Type': 'forward', 'TargetGroupArn': tg_arn}]
            )
            print("Listener na porta 80 vinculado ao Target Group.")
        else:
            print("Listener na porta 80 já existe. Pulando criação...")

        print("Camada de computação configurada com sucesso.")
    def seed_relational_database(self):
        print("\n--- 5. Populando o Banco de Dados ---")
        sg_rds_id = self.resource_ids['SgRds']
        
        # 5.1 Abre a porta temporariamente para o script injetar os dados
        print("Abrindo firewall para injeção de dados...")
        try:
            self.ec2.authorize_security_group_ingress(
                GroupId=sg_rds_id,
                IpPermissions=[{'IpProtocol': 'tcp', 'FromPort': 5432, 'ToPort': 5432, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}]
            )
            time.sleep(10) # Tempo para a regra propagar na AWS
        except ClientError as e:
            if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
                pass # A porta já estava aberta
            else:
                raise e

        # 5.2 Conecta e Popula
        print(f"Conectando no PostgreSQL em {self.db_endpoint}...")
        try:
            conn = psycopg2.connect(host=self.db_endpoint, port=5432, dbname=self.db_name, user=self.db_user, password=self.db_pass)
            cur = conn.cursor()
            
            cur.execute("""
                DROP TABLE IF EXISTS pedidos CASCADE;
                DROP TABLE IF EXISTS entregadores CASCADE;
                DROP TABLE IF EXISTS restaurantes CASCADE;
                DROP TABLE IF EXISTS clientes CASCADE;

                CREATE TABLE clientes (id_cliente SERIAL PRIMARY KEY, nome VARCHAR(80), email VARCHAR(80), telefone VARCHAR(20), latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);
                CREATE TABLE restaurantes (id_restaurante SERIAL PRIMARY KEY, nome VARCHAR(80), tipo_cozinha VARCHAR(40), latitude DOUBLE PRECISION, longitude DOUBLE PRECISION);
                CREATE TABLE entregadores (id_entregador SERIAL PRIMARY KEY, nome VARCHAR(80), tipo_veiculo VARCHAR(30), latitude_inicial DOUBLE PRECISION, longitude_inicial DOUBLE PRECISION, status_ocupado BOOLEAN DEFAULT FALSE);
                CREATE TABLE pedidos (id_pedido SERIAL PRIMARY KEY, id_cliente INT REFERENCES clientes(id_cliente), id_restaurante INT REFERENCES restaurantes(id_restaurante), id_entregador INT REFERENCES entregadores(id_entregador), data_criacao TIMESTAMP DEFAULT NOW());
                
                CREATE INDEX idx_entregador ON entregadores(status_ocupado) WHERE status_ocupado = FALSE;
            """)
            
            # Gerador simples de coordenadas na grande SP
            def get_lat_lon(): return random.uniform(-23.7, -23.4), random.uniform(-46.8, -46.3)
            
            print("Inserindo 1000 Clientes, 50 Restaurantes e 3000 Entregadores...")

            clientes, restaurantes, entregadores = gerar_dados_falsos(1000, 50, 3000)
            psycopg2.extras.execute_values(cur, "INSERT INTO clientes (nome, email, telefone, latitude, longitude) VALUES %s", clientes, page_size=500)
            psycopg2.extras.execute_values(cur, "INSERT INTO restaurantes (nome, tipo_cozinha, latitude, longitude) VALUES %s", restaurantes, page_size=100)
            psycopg2.extras.execute_values(cur, "INSERT INTO entregadores (nome, tipo_veiculo, latitude_inicial, longitude_inicial, status_ocupado) VALUES %s", entregadores, page_size=2000)

            conn.commit()
            cur.close()
            conn.close()
            print("Banco de dados estruturado e populado com sucesso!")

        except Exception as e:
            print(f"Erro ao popular banco: {e}")
        finally:
            # 5.3 Tranca a porta novamente, garantindo o isolamento da arquitetura (Obrigatório para segurança)
            print("Fechando firewall novamente (Garantindo o isolamento da arquitetura)...")
            try:
                self.ec2.revoke_security_group_ingress(
                    GroupId=sg_rds_id,
                    IpPermissions=[{'IpProtocol': 'tcp', 'FromPort': 5432, 'ToPort': 5432, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}]
                )
            except Exception as e:
                print(f"Aviso ao fechar porta: {e}")
    

    def run(self):
        try:
            self.create_network_and_security()
            self.create_storage_and_nosql()
            self.prepare_and_upload_graph()
            self.create_rds_database()
            self.seed_relational_database()
            self.create_compute_layer()
            print("\nINFRAESTRUTURA COMPLETA CRIADA COM SUCESSO!")
        except Exception as e:
            print(f"\nERRO DURANTE O DEPLOY: {e}")
            print("Verifique se as credenciais do Voclabs (Session Token) estão válidas e não expiraram.")

if __name__ == "__main__":
    deployer = DijkFoodDeployer()
    deployer.run()
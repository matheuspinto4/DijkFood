from fake_data import *

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
    
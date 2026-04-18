import psycopg2
import boto3
import os

# ---------------------------------------------------------------------------
# Configurações de Conexão
# ---------------------------------------------------------------------------
# RDS
DB_HOST = "dijkfood-primary.cjgmo62ku36m.us-east-1.rds.amazonaws.com"
DB_USER = "dijk_admin"
DB_PASS = "DijkFood2026!Cloud"
DB_NAME = "dijkfooddb"

# DynamoDB
REGION = "us-east-1"
dynamodb = boto3.resource("dynamodb", region_name=REGION)

TABELAS_DYNAMO = [
    {"nome": "dijkfood-historico-eventos", "chaves": ["id_pedido", "timestamp"]},
    {"nome": "dijkfood-telemetria-entregadores", "chaves": ["id_entregador", "timestamp"]},
    {"nome": "dijkfood-alocacao-entregadores", "chaves": ["id_entregador", "timestamp"]}
]

# ---------------------------------------------------------------------------
# Função para limpar o DynamoDB (Scan + Batch Delete)
# ---------------------------------------------------------------------------
def limpar_dynamodb(tabela_info):
    nome_tabela = tabela_info["nome"]
    chaves = tabela_info["chaves"]
    
    print(f"[DYNAMODB] Iniciando varredura na tabela '{nome_tabela}'...")
    table = dynamodb.Table(nome_tabela)
    
    # Faz o Scan puxando APENAS as chaves primárias (para economizar banda e ser muito mais rápido)
    expressao_projecao = ", ".join(chaves)
    scan = table.scan(ProjectionExpression=expressao_projecao)
    
    itens_deletados = 0
    # O batch_writer gerencia a fila e manda deletar de 25 em 25 automaticamente
    with table.batch_writer() as batch:
        for item in scan.get('Items', []):
            batch.delete_item(Key={k: item[k] for k in chaves})
            itens_deletados += 1
            
        # O DynamoDB retorna no máximo 1MB por Scan. Se houver mais dados, paginamos:
        while 'LastEvaluatedKey' in scan:
            scan = table.scan(
                ProjectionExpression=expressao_projecao, 
                ExclusiveStartKey=scan['LastEvaluatedKey']
            )
            for item in scan.get('Items', []):
                batch.delete_item(Key={k: item[k] for k in chaves})
                itens_deletados += 1
                
    print(f"[DYNAMODB] Sucesso! {itens_deletados} itens apagados da tabela '{nome_tabela}'.")

# ---------------------------------------------------------------------------
# Execução Principal
# ---------------------------------------------------------------------------
def main():
    print("Iniciando o Protocolo de Limpeza Total da Arquitetura DijkFood...\n")
    
    # 1. Limpeza do RDS (PostgreSQL)
    try:
        print("[RDS] Conectando ao Banco Relacional...")
        conn = psycopg2.connect(host=DB_HOST, port=5432, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        conn.autocommit = True
        
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE pedidos, entregadores, restaurantes, clientes RESTART IDENTITY CASCADE;")
            
        print("[RDS] Sucesso! O Banco de Dados Relacional está vazio e com IDs resetados.")
        conn.close()
    except Exception as e:
        print(f"[ERRO RDS] Falha ao limpar o PostgreSQL: {e}")

    print("-" * 50)

    # 2. Limpeza do DynamoDB (NoSQL)
    try:
        for tabela in TABELAS_DYNAMO:
            limpar_dynamodb(tabela)
    except Exception as e:
         print(f"[ERRO DYNAMODB] Falha ao limpar o DynamoDB: {e}")
         
    print("\n[LIMPEZA CONCLUÍDA] O sistema está 100% zerado e pronto para uma nova simulação limpa!")

if __name__ == "__main__":
    main()
# DijkFood - Plataforma de Delivery em Nuvem

**Projeto de Computação em Nuvem - FGV**

## Resumo

O DijkFood é um serviço de delivery de comida fictício construído para operar em nuvem com foco em escalabilidade, atomicidade transacional, alta disponibilidade, independência de componentes e durabilidade dos dados. A plataforma foi desenvolvida para gerenciar o ciclo de vida completo de pedidos, calcular rotas sobre o grafo viário de São Paulo e processar a telemetria em tempo real dos entregadores simultaneamente. 

A solução utiliza uma arquitetura de serviços na AWS. 

### Serviços AWS Utilizados
* Amazon ECS (Fargate);
* Amazon RDS (Multi-AZ);
* Amazon DynamoDB (On-Demand);
* Amazon S3.


## Como Executar:
Primeiramente, garantir que todas as bibliotecas e suas dependências estão instaladas e configuradas:

```bash
pip install -r requirements.txt
```

A implantação do sistema, a população de dados, o teste de carga e a destruição do sistema são gerenciados de forma automatizada pelo script `deploy.py`.

O script é dividido em 4 etapas principais:
1. **`allocate`**: Cria toda a infraestrutura na AWS e as tabelas vazias no Banco de Dados.
2. **`populate`**: Baixa o Grafo Viário (OSMnx) da cidade de São Paulo e envia para o bucket S3.
3. **`simulator`**: Executa a simulação assíncrona de tráfego utilizando a URL dinâmica gerada pelo ALB.
4. **`destroy`**: Apaga TODOS os recursos na AWS, evitando custos residuais.

### Execução Completa
Para rodar todo o ciclo de vida do projeto de forma sequencial (da criação à destruição), basta executar:

```bash
python deploy.py
```

### Execução por Etapas (Steps)
Caso precise debugar ou executar um passo isoladamente, utilize o argumento --step seguido do nome do comando.

### Grupo 
* Beatriz dos Santos Marques
* Carlos Daniel de Souza Lima
* Matheus Vilarino de Souza Pinto
* Walléria Simões Correia

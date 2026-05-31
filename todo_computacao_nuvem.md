
1. Passar o código para o terraform
2. Adaptar o broker SQS que leva os pedidos da API até o worker:

   1. Criar o SQS
   2. Ligar ele nos conteiners da API e do WORKER
   3. Fazer o worker parar de ler do RDS
   4. Worker passa a escrever no dynamodb a associação de um pedido com um entregador
   5. worker atualiza o RDS, associando o pedido ao entregador
3. MSK \& API Gateway:

   1. Criar a instância MSK
   2. Ligar os sensores de leitura dela nos contêineres da API e do WORKER
   3. Levantar a API Gayteway
   4. Configurar o LAMBDA
   5. Ligar o MSK ao LAMBDA 
   6. Ligar LAMBDA ao API Gateway
4. Redshift

   1. Levantar a instância do Redshift
   2. Ligar o Redshift ao MSK 
5. Interface para o Dashbord em Tempo real

   1. Desenvolver uma interface simples com algumas métricas
   2. Ligar os valores das variáveis à API Gateway
6. Interface para o Dash analítico do Redshift

   1. Desenvolver a interface
   2. Ligar os valores da interface ao Redshift
7. Ajustar o Simulador

   1. Fazer com que o entregador receba a ordem de busca e entrega de pedido através da API Gateway
8. Escrever o relatório

   1. Alternativas consideradas para a arquitetura 
   2. Justificativas para as escolhas da arquitetura e do terraform
   3. Explicação da camada analítica (redshift)
   4. Explicação do Dashboard em tempo real e suas funcionalidades
   5. Resultados dos experimentos na simulação (teste de carga)
   6. Objetivos atendidos \*\* ou não :( \*\*
   7. Análise de custos 








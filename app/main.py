import logging
import os
import uuid
import boto3
from enum import Enum
from datetime import datetime
from typing import Annotated

from pydantic import BaseModel
from fastapi import Depends, FastAPI, HTTPException, Query
from sqlmodel import Field, Session, SQLModel, create_engine, select

# Configurações do DynamoDB
DYNAMODB_REGION = os.getenv("AWS_REGION", "us-east-1")
dynamodb = boto3.resource("dynamodb", region_name=DYNAMODB_REGION)
NOME_TABELA_EVENTOS = os.getenv("DDB_EVENTOS", "dijkfood-historico-eventos")
tabela_eventos = dynamodb.Table(NOME_TABELA_EVENTOS)

# Configuração de logs para monitorar a API na nuvem
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class Cliente(SQLModel, table=True):
    __tablename__ = "clientes"
    id_cliente: int | None = Field(default=None, primary_key=True)
    nome: str
    email: str
    telefone: str
    latitude: float
    longitude: float

class Restaurante(SQLModel, table=True):
    __tablename__ = "restaurantes"
    id_restaurante: int | None = Field(default=None, primary_key=True)
    nome: str
    tipo_cozinha: str
    latitude: float
    longitude: float

class Entregador(SQLModel, table=True):
    __tablename__ = "entregadores"
    id_entregador: int | None = Field(default=None, primary_key=True)
    nome: str
    tipo_veiculo: str
    latitude_inicial: float
    longitude_inicial: float
    status_ocupado: bool = Field(default=False)

class StatusPedido(str, Enum):
    CONFIRMED = "CONFIRMED"
    PREPARING = "PREPARING"
    READY_FOR_PICKUP = "READY_FOR_PICKUP"
    PICKED_UP = "PICKED_UP"
    IN_TRANSIT = "IN_TRANSIT"
    DELIVERED = "DELIVERED"

TRANSICOES_VALIDAS = {
    StatusPedido.CONFIRMED: StatusPedido.PREPARING,
    StatusPedido.PREPARING: StatusPedido.READY_FOR_PICKUP,
    StatusPedido.READY_FOR_PICKUP: StatusPedido.PICKED_UP,
    StatusPedido.PICKED_UP: StatusPedido.IN_TRANSIT,
    StatusPedido.IN_TRANSIT: StatusPedido.DELIVERED,
}

class PedidoStatusUpdate(BaseModel):
    novo_status: StatusPedido

class Pedido(SQLModel, table=True):
    __tablename__ = "pedidos"
    id_pedido: int | None = Field(default=None, primary_key=True)
    id_cliente: int = Field(foreign_key="clientes.id_cliente")
    id_restaurante: int = Field(foreign_key="restaurantes.id_restaurante")
    id_entregador: int = Field(foreign_key="entregadores.id_entregador")
    lista_itens: str | None = None 
    status: str = Field(default=StatusPedido.CONFIRMED.value) 
    data_criacao: datetime = Field(default_factory=datetime.now)

class PedidoCreate(BaseModel):
    id_cliente: int
    id_restaurante: int

# Conexão com a AWS
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL) if DATABASE_URL else None

def get_session():
    with Session(engine) as session:
        yield session

SessionDep = Annotated[Session, Depends(get_session)]
app = FastAPI(title="DijkFood API - Produção")

# Criação das rotas
@app.get("/clientes/", response_model=list[Cliente])
def listar_clientes(session: SessionDep, offset: int = 0, limit: int = 10):
    return session.exec(select(Cliente).offset(offset).limit(limit)).all()

@app.get("/restaurantes/", response_model=list[Restaurante])
def listar_restaurantes(session: SessionDep, offset: int = 0, limit: int = 10):
    return session.exec(select(Restaurante).offset(offset).limit(limit)).all()

@app.get("/entregadores/", response_model=list[Entregador])
def listar_entregadores(session: SessionDep, offset: int = 0, limit: int = 10):
    return session.exec(select(Entregador).offset(offset).limit(limit)).all()

@app.get("/pedidos/", response_model=list[Pedido])
def listar_pedidos(session: SessionDep, offset: int = 0, limit: int = 10):
    # Requisito: Administrador deve consultar o histórico de pedidos
    return session.exec(select(Pedido).offset(offset).limit(limit)).all()

@app.get("/pedidos/{id_pedido}", response_model=Pedido)
def consultar_pedido(id_pedido: int, session: SessionDep):
    pedido = session.get(Pedido, id_pedido)
    if not pedido:
        raise HTTPException(status_code=404, detail="Pedido não encontrado")
    return pedido

@app.post("/pedidos/", response_model=Pedido)
def criar_pedido(pedido_in: PedidoCreate, session: SessionDep):
    logger.info(f"Iniciando criação de pedido para Cliente {pedido_in.id_cliente}")

    # Verifica se Cliente existe
    cliente = session.get(Cliente, pedido_in.id_cliente)
    if not cliente:
        raise HTTPException(status_code=404, detail="Cliente não encontrado")

    # Verifica se Restaurante existe
    restaurante = session.get(Restaurante, pedido_in.id_restaurante)
    if not restaurante:
        raise HTTPException(status_code=404, detail="Restaurante não encontrado")

    # Busca o primeiro entregador disponível (status_ocupado = False)
    entregador = session.exec(
        select(Entregador).where(Entregador.status_ocupado == False)
    ).first()

    if not entregador:
        raise HTTPException(status_code=400, detail="Nenhum entregador disponível no momento")

    try:
        entregador.status_ocupado = True
        session.add(entregador)

        novo_pedido = Pedido(
            id_cliente=cliente.id_cliente,
            id_restaurante=restaurante.id_restaurante,
            id_entregador=entregador.id_entregador
        )
        session.add(novo_pedido)

        # Só salva se der tudo certo 
        session.commit()
        session.refresh(novo_pedido)
        
        logger.info(f"Pedido {novo_pedido.id_pedido} criado com sucesso!")
        return novo_pedido

    except Exception as e:
        session.rollback() # Desfaz qualquer mudança parcial em caso de erro
        logger.error(f"Erro atômico ao criar pedido: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao processar o pedido")
    
@app.patch("/pedidos/{id_pedido}/status", response_model=Pedido)
def atualizar_status_pedido(id_pedido: int, update_data: PedidoStatusUpdate, session: SessionDep):
    # 1. Busca o pedido no RDS
    pedido = session.get(Pedido, id_pedido)
    if not pedido:
        raise HTTPException(status_code=404, detail="Pedido não encontrado")

    status_atual = pedido.status
    novo_status = update_data.novo_status.value

    # 2. Validação estrita da Máquina de Estados
    status_esperado = TRANSICOES_VALIDAS.get(status_atual)
    if status_esperado != novo_status:
        logger.warning(f"Tentativa de transição ilegal no Pedido {id_pedido}: {status_atual} -> {novo_status}")
        raise HTTPException(
            status_code=400,
            detail=f"Transição inválida. O pedido está '{status_atual}', o próximo status deve ser '{status_esperado}'"
        )

    try:
        # 3. Atualiza o status principal no AWS RDS
        pedido.status = novo_status
        session.add(pedido)
        session.commit()
        session.refresh(pedido)

        # 4. Grava o evento histórico no AWS DynamoDB
        # Usamos o formato "PEDIDO#123" para bater com a chave de partição (Partition Key) do seu script
        tabela_eventos.put_item(
            Item={
                "id_pedido": f"PEDIDO#{id_pedido}",
                "timestamp": datetime.utcnow().isoformat(),
                "status": novo_status,
                "event_id": uuid.uuid4().hex[:8]
            }
        )

        logger.info(f"Pedido {id_pedido} avançou para {novo_status}")
        return pedido

    except Exception as e:
        session.rollback()
        logger.error(f"Falha ao processar mudança de status do pedido {id_pedido}: {e}")
        raise HTTPException(status_code=500, detail="Erro interno de comunicação com os bancos de dados.")
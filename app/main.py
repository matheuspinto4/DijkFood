import logging
import os
import uuid
import boto3
import json
from enum import Enum
from datetime import datetime, date, time as datetime_time
from typing import Annotated

from sqlmodel import Field, Session, SQLModel, create_engine, select
from boto3.dynamodb.conditions import Key
from pydantic import BaseModel
from fastapi import Depends, FastAPI, HTTPException
from botocore.config import Config

# ---------------------------------------------------------------------------
# Configurações AWS & DynamoDB
# ---------------------------------------------------------------------------
DYNAMODB_REGION = os.getenv("AWS_REGION", "us-east-1")
boto_config = Config(max_pool_connections=50) # Libera 50 conexões simultâneas
dynamodb = boto3.resource("dynamodb", region_name=DYNAMODB_REGION, config=boto_config)

NOME_TABELA_EVENTOS = os.getenv("DDB_EVENTOS", "dijkfood-historico-eventos")
tabela_eventos = dynamodb.Table(NOME_TABELA_EVENTOS)

NOME_TABELA_TELEMETRIA = os.getenv("DDB_TELEMETRIA", "dijkfood-telemetria-entregadores")
tabela_telemetria = dynamodb.Table(NOME_TABELA_TELEMETRIA)

NOME_TABELA_ALOCACOES = os.getenv("DDB_ALOCACOES", "dijkfood-alocacao-entregadores")
tabela_alocacoes= dynamodb.Table(NOME_TABELA_ALOCACOES)

# Configuração de logs para monitorar a API na nuvem (CloudWatch)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Conexão com Banco de Dados RDS (PostgreSQL)
# ---------------------------------------------------------------------------
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

# CORREÇÃO: Monta a URL dinamicamente caso as variáveis do ECS estejam presentes
DATABASE_URL = os.getenv("DATABASE_URL") or (
    f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}" 
    if DB_HOST else None
)

engine = create_engine(DATABASE_URL) if DATABASE_URL else None

def get_session():
    with Session(engine) as session:
        yield session

SessionDep = Annotated[Session, Depends(get_session)]

# ---------------------------------------------------------------------------
# Modelos (SQLModel / Pydantic)
# ---------------------------------------------------------------------------
class Cliente(SQLModel, table=True):
    __tablename__ = "clientes"
    id_cliente: int | None = Field(default=None, primary_key=True)
    nome: str
    email: str
    telefone: str
    latitude: float
    longitude: float

class ClienteCreate(BaseModel):
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

class RestauranteCreate(BaseModel):
    nome: str
    tipo_cozinha: str
    latitude: float
    longitude: float

class Entregador(SQLModel, table=True):
    __tablename__ = "entregadores"
    id_entregador: int | None = Field(default=None, primary_key=True)
    nome: str
    tipo_veiculo: str
    status: str | None = Field(default="AVAILABLE") 
    latitude: float | None = None
    longitude: float | None = None

class EntregadorCreate(BaseModel):
    nome: str
    tipo_veiculo: str
    latitude: float
    longitude: float

class PosicaoUpdate(BaseModel):
    latitude: float
    longitude: float

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
    
    # CORREÇÃO CRÍTICA: O pedido agora aceita nascer sem entregador!
    id_entregador: int | None = Field(default=None, foreign_key="entregadores.id_entregador")
    
    lista_itens: str | None = None
    status: str = Field(default="CONFIRMED")
    data: date | None = None
    horario: datetime_time | None = None

class PedidoCreate(BaseModel):
    id_cliente: int
    id_restaurante: int
    lista_itens: list[dict]

# ---------------------------------------------------------------------------
# Instância da API
# ---------------------------------------------------------------------------
app = FastAPI(title="DijkFood API - Produção")

# ---------------------------------------------------------------------------
# Rotas: Clientes, Restaurantes e Entregadores
# ---------------------------------------------------------------------------
@app.get("/clientes/", response_model=list[Cliente])
def listar_clientes(session: SessionDep, offset: int = 0, limit: int = 10):
    return session.exec(select(Cliente).offset(offset).limit(limit)).all()

@app.post("/clientes/", response_model=Cliente)
def criar_cliente(cliente_in: ClienteCreate, session: SessionDep):
    try:
        # Uso do model_dump() deixa o código mais limpo
        novo_cliente = Cliente(**cliente_in.model_dump())
        session.add(novo_cliente)
        session.commit()
        session.refresh(novo_cliente)
        return novo_cliente
    except Exception as e:
        session.rollback()
        logger.error(f"Erro ao criar cliente: {e}")
        raise HTTPException(status_code=500, detail="Erro interno")

@app.get("/restaurantes/", response_model=list[Restaurante])
def listar_restaurantes(session: SessionDep, offset: int = 0, limit: int = 10):
    return session.exec(select(Restaurante).offset(offset).limit(limit)).all()

@app.post("/restaurantes/", response_model=Restaurante)
def criar_restaurante(restaurante_in: RestauranteCreate, session: SessionDep):
    try:
        novo_restaurante = Restaurante(**restaurante_in.model_dump())
        session.add(novo_restaurante)
        session.commit()
        session.refresh(novo_restaurante)
        return novo_restaurante
    except Exception as e:
        session.rollback()
        logger.error(f"Erro ao criar restaurante: {e}")
        raise HTTPException(status_code=500, detail="Erro interno")

@app.get("/entregadores/", response_model=list[Entregador])
def listar_entregadores(session: SessionDep, offset: int = 0, limit: int = 10):
    return session.exec(select(Entregador).offset(offset).limit(limit)).all()

@app.post("/entregadores/", response_model=Entregador)
def criar_entregador(entregador_in: EntregadorCreate, session: SessionDep):
    try:
        novo_entregador = Entregador(**entregador_in.model_dump())
        session.add(novo_entregador)
        session.commit()
        session.refresh(novo_entregador)
        return novo_entregador
    except Exception as e:
        session.rollback()
        logger.error(f"Erro ao criar entregador: {e}")
        raise HTTPException(status_code=500, detail="Erro interno")

@app.post("/entregadores/{id_entregador}/posicao")
def atualizar_posicao(id_entregador: int, posicao: PosicaoUpdate):
    try:
        tabela_telemetria.put_item(
            Item={
                "id_entregador": str(id_entregador), 
                "timestamp": datetime.utcnow().isoformat(),
                "latitude": str(posicao.latitude),
                "longitude": str(posicao.longitude)
            }
        )
        return {"status": "Posição recebida"}
    except Exception as e:
        logger.error(f"Erro ao salvar telemetria: {e}")
        raise HTTPException(status_code=500, detail="Erro ao salvar posição")

# ---------------------------------------------------------------------------
# Rotas: Pedidos
# ---------------------------------------------------------------------------
@app.get("/pedidos/", response_model=list[Pedido])
def listar_pedidos(session: SessionDep, offset: int = 0, limit: int = 10):
    return session.exec(select(Pedido).offset(offset).limit(limit)).all()

@app.get("/pedidos/{id_pedido}", response_model=Pedido)
def consultar_pedido(id_pedido: int, session: SessionDep):
    pedido = session.get(Pedido, id_pedido)
    if not pedido:
        raise HTTPException(status_code=404, detail="Pedido não encontrado")
    return pedido

@app.post("/pedidos/", response_model=Pedido)
def criar_pedido(pedido_in: PedidoCreate, session: SessionDep):
    cliente = session.get(Cliente, pedido_in.id_cliente)
    restaurante = session.get(Restaurante, pedido_in.id_restaurante)
    if not cliente or not restaurante:
        raise HTTPException(status_code=404, detail="Cliente ou Restaurante não encontrado")

    try:
        novo_pedido = Pedido(
            id_cliente=cliente.id_cliente,
            id_restaurante=restaurante.id_restaurante,
            id_entregador=None, # O pedido entra na fila órfão, o Worker assume depois!
            lista_itens=json.dumps(pedido_in.lista_itens)
        )
        session.add(novo_pedido)
        session.flush()

        tabela_eventos.put_item(
            Item={
                "id_pedido": str(novo_pedido.id_pedido),
                "timestamp": datetime.utcnow().isoformat(),
                "status": novo_pedido.status,
                "event_id": uuid.uuid4().hex[:8],
                "id_entregador": None
            }
        )
        
        session.commit()
        session.refresh(novo_pedido)
        return novo_pedido

    except Exception as e:
        session.rollback()
        logger.error(f"Erro ao criar pedido: {e}")
        raise HTTPException(status_code=500, detail="Erro ao processar o pedido")

@app.get("/pedidos/{id_pedido}/acompanhamento")
def acompanhar_pedido(id_pedido: int): 
    # 1. Busca o status atualizado do pedido no DynamoDB
    try:
        resposta = tabela_eventos.query(
            KeyConditionExpression=Key('id_pedido').eq(str(id_pedido)),
            ScanIndexForward=False, 
            Limit=1
        )
        itens = resposta.get('Items', [])
    except Exception as e:
        logger.error(f"Falha real de comunicação com DynamoDB: {e}")
        raise HTTPException(status_code=500, detail="Erro ao conectar com o histórico de eventos.")

    # A validação 404 agora está 100% protegida fora do try
    if not itens:
        raise HTTPException(status_code=404, detail="Nenhum status encontrado para este pedido.")

    # ATENÇÃO: 'pedido_dict' é um Dicionário JSON, usamos .get() em vez de pontos
    pedido_dict = itens[0]
    status_atual = pedido_dict.get("status")
    id_entregador_str = pedido_dict.get("id_entregador")

    # 2. Busca a última posição do entregador no DynamoDB
    ultima_posicao = None
    if id_entregador_str and id_entregador_str != "None":
        try:
            resp_telemetria = tabela_telemetria.query(
                KeyConditionExpression=Key('id_entregador').eq(id_entregador_str),
                ScanIndexForward=False,
                Limit=1 
            )
            itens_pos = resp_telemetria.get('Items', [])
            if itens_pos:
                ultima_posicao = {
                    "latitude": float(itens_pos[0]["latitude"]),
                    "longitude": float(itens_pos[0]["longitude"]),
                    "ultima_atualizacao": itens_pos[0]["timestamp"]
                }
        except Exception as e:
            logger.error(f"Erro ao buscar telemetria, mas ignorando para o usuário: {e}")
            pass

    # 3. Junta tudo e devolve para o cliente
    return {
        "id_pedido": id_pedido,
        "status": status_atual,
        "id_entregador": int(id_entregador_str) if id_entregador_str and id_entregador_str != "None" else None,
        "posicao_entregador": ultima_posicao
    }

@app.patch("/pedidos/{id_pedido}/status", response_model=Pedido)
def atualizar_status_pedido(id_pedido: int, update_data: PedidoStatusUpdate, session: SessionDep):
    pedido = session.get(Pedido, id_pedido)
    if not pedido:
        raise HTTPException(status_code=404, detail="Pedido não encontrado")

    # Se não for a criação inicial (CONFIRMED), é exigido que já exista um entregador associado
    if update_data.novo_status not in (StatusPedido.CONFIRMED, StatusPedido.READY_FOR_PICKUP, StatusPedido.PREPARING) and pedido.id_entregador is None:
        raise HTTPException(status_code=400, detail="O pedido ainda não possui entregador.")

    status_atual = pedido.status
    novo_status = update_data.novo_status.value
    status_esperado = TRANSICOES_VALIDAS.get(status_atual)
    
    if status_esperado != novo_status:
        raise HTTPException(
            status_code=400,
            detail=f"Transição inválida. De {status_atual} deve ir para {status_esperado}"
        )

    try:
        pedido.status = novo_status
        session.add(pedido)
        session.commit()
        session.refresh(pedido)

        # 4. Grava o evento histórico no AWS DynamoDB
        # Usamos o formato "PEDIDO#123" para bater com a chave de partição (Partition Key) do seu script
        tabela_eventos.put_item(
            Item={
                "id_pedido": str(id_pedido),
                "timestamp": datetime.utcnow().isoformat(),
                "status": novo_status,
                "event_id": uuid.uuid4().hex[:8],
                "id_entregador": str(pedido.id_entregador)
            }
        )
        return pedido

    except Exception as e:
        session.rollback()
        logger.error(f"Erro ao atualizar status: {e}")
        raise HTTPException(status_code=500, detail="Erro de comunicação com banco de dados")

@app.get("/pedidos/{id_pedido}/historico")
def historico_pedido(id_pedido: int):
    # Administrador consultando histórico cronológico
    try:
        resposta = tabela_eventos.query(
            # Busca todos os eventos que pertencem a este pedido
            KeyConditionExpression=Key('id_pedido').eq(str(id_pedido)),
            # ScanIndexForward=True garante a ordem do mais antigo para o mais novo
            ScanIndexForward=True 
        )
        itens = resposta.get('Items', [])
        
        if not itens:
            raise HTTPException(status_code=404, detail="Nenhum histórico encontrado para este pedido.")
            
        return itens
    except Exception as e:
        logger.error(f"Erro ao buscar histórico do pedido {id_pedido}: {e}")
        raise HTTPException(status_code=500, detail="Erro ao conectar com o histórico de eventos.")
    

@app.get("/entregadors/{id_entregador}/acompanhamento")
def consultar_alocacao(id_entregador: int): 
    # 1. Busca o status atualizado do entregador no DynamoDB
    try:
        resposta = tabela_alocacoes.query(
            KeyConditionExpression=Key('id_entregador').eq(str(id_entregador)),
            ScanIndexForward=False, 
            Limit=1
        )
        itens = resposta.get('Items', [])
    except Exception as e:
        logger.error(f"Falha real de comunicação com DynamoDB: {e}")
        raise HTTPException(status_code=500, detail="Erro ao conectar com o histórico de eventos.")

    # A validação 404 agora está 100% protegida fora do try
    if not itens:
        raise HTTPException(status_code=404, detail="Nenhum status encontrado para este entregador.")

    # ATENÇÃO: 'entregador_dict' é um Dicionário JSON, usamos .get() em vez de pontos
    entregador_dict = itens[0]
    status_atual = entregador_dict.get("status")
    id_entregador_str = entregador_dict.get("id_entregador")

    # 2. Busca a última posição do entregador no DynamoDB
    ultima_posicao = None
    if id_entregador_str and id_entregador_str != "None":
        try:
            resp_telemetria = tabela_telemetria.query(
                KeyConditionExpression=Key('id_entregador').eq(id_entregador_str),
                ScanIndexForward=False,
                Limit=1 
            )
            itens_pos = resp_telemetria.get('Items', [])
            if itens_pos:
                ultima_posicao = {
                    "latitude": float(itens_pos[0]["latitude"]),
                    "longitude": float(itens_pos[0]["longitude"]),
                    "ultima_atualizacao": itens_pos[0]["timestamp"]
                }
        except Exception as e:
            logger.error(f"Erro ao buscar telemetria, mas ignorando para o usuário: {e}")
            pass

    # 3. Junta tudo e devolve para o cliente
    return {
        "id_entregador": id_entregador,
        "status": status_atual,
        "id_entregador": int(id_entregador_str) if id_entregador_str and id_entregador_str != "None" else None,
        "posicao_entregador": ultima_posicao
    }
    
@app.post("/alocacoes/{id_entregador}")
def desativar_alocacao(id_entregador, id_pedido):
    try:
        tabela_alocacoes.put_item(
            Item={
                "id_entregador": str(id_entregador),
                "timestamp": datetime.utcnow().isoformat(),
                "status": "INATIVA",
                "id_pedido": id_pedido,
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail="Erro ao desativar a alocacao")

# Rotas Otimizadas para o Simulador (Bulk Insert)
@app.post("/clientes/bulk")
def criar_clientes_bulk(clientes_in: list[ClienteCreate], session: SessionDep):
    novos = [Cliente(nome=c.nome, email=c.email, telefone=c.telefone, latitude=c.latitude, longitude=c.longitude) for c in clientes_in]
    session.add_all(novos)
    session.commit()
    return {"status": "ok", "inseridos": len(novos)}

@app.post("/restaurantes/bulk")
def criar_restaurantes_bulk(restaurantes_in: list[RestauranteCreate], session: SessionDep):
    novos = [Restaurante(nome=r.nome, tipo_cozinha=r.tipo_cozinha, latitude=r.latitude, longitude=r.longitude) for r in restaurantes_in]
    session.add_all(novos)
    session.commit()
    return {"status": "ok", "inseridos": len(novos)}

@app.post("/entregadores/bulk")
def criar_entregadores_bulk(entregadores_in: list[EntregadorCreate], session: SessionDep):
    novos = [Entregador(nome=e.nome, tipo_veiculo=e.tipo_veiculo, latitude=e.latitude, longitude=e.longitude) for e in entregadores_in]
    session.add_all(novos)
    session.commit()
    return {"status": "ok", "inseridos": len(novos)}
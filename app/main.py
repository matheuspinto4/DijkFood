import logging
import os
import uuid
import boto3
import json
from enum import Enum
from datetime import datetime
from typing import Annotated

from datetime import date, time as datetime_time
from sqlmodel import Field, SQLModel
from boto3.dynamodb.conditions import Key
from pydantic import BaseModel
from fastapi import Depends, FastAPI, HTTPException, Query
from sqlmodel import Field, Session, SQLModel, create_engine, select

from botocore.config import Config

# Configurações do DynamoDB
DYNAMODB_REGION = os.getenv("AWS_REGION", "us-east-1")

boto_config = Config(max_pool_connections=50) # Libera 50 conexões simultâneas

dynamodb = boto3.resource("dynamodb", region_name=DYNAMODB_REGION, config=boto_config)
NOME_TABELA_EVENTOS = os.getenv("DDB_EVENTOS", "dijkfood-historico-eventos")
tabela_eventos = dynamodb.Table(NOME_TABELA_EVENTOS)
NOME_TABELA_TELEMETRIA = os.getenv("DDB_TELEMETRIA", "dijkfood-telemetria-entregadores")
tabela_telemetria = dynamodb.Table(NOME_TABELA_TELEMETRIA)

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
    id_entregador: int = Field(default=None, foreign_key="entregadores.id_entregador")
    lista_itens: str | None = None
    status: str = Field(default="CONFIRMED")
    data: date | None = None
    horario: datetime_time | None = None

class PedidoCreate(BaseModel):
    id_cliente: int
    id_restaurante: int
    lista_itens: list[dict]

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

@app.post("/clientes/", response_model=Cliente)
def criar_cliente(cliente_in: ClienteCreate, session: SessionDep):
    logger.info(f"Iniciando criação do cliente {cliente_in.nome}")

    try:
        novo_cliente = Cliente(
                nome = cliente_in.nome,
                email = cliente_in.email,
                telefone = cliente_in.telefone,
                latitude = cliente_in.latitude,
                longitude = cliente_in.longitude
        )
        session.add(novo_cliente)

        # Só salva se der tudo certo 
        session.commit()
        session.refresh(novo_cliente)

        logger.info(f"Cliente {novo_cliente.id_cliente} criado com sucesso!")
        return novo_cliente

    except Exception as e:
        session.rollback() # Desfaz qualquer mudança parcial em caso de erro
        logger.error(f"Erro atômico ao criar cliente: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao processar o cliente")

@app.get("/restaurantes/", response_model=list[Restaurante])
def listar_restaurantes(session: SessionDep, offset: int = 0, limit: int = 10):
    return session.exec(select(Restaurante).offset(offset).limit(limit)).all()

@app.post("/restaurantes/", response_model=Restaurante)
def criar_restaurante(restaurante_in: RestauranteCreate, session: SessionDep):
    logger.info(f"Iniciando criação do restaurante {restaurante_in.nome}")

    try:
        novo_restaurante = Restaurante(
                nome = restaurante_in.nome,
                tipo_cozinha = restaurante_in.tipo_cozinha,
                latitude = restaurante_in.latitude,
                longitude = restaurante_in.longitude
        )
        session.add(novo_restaurante)

        # Só salva se der tudo certo 
        session.commit()
        session.refresh(novo_restaurante)

        logger.info(f"Restaurante {novo_restaurante.id_restaurante} criado com sucesso!")
        return novo_restaurante

    except Exception as e:
        session.rollback() # Desfaz qualquer mudança parcial em caso de erro
        logger.error(f"Erro atômico ao criar restaurante: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao processar o restaurante")

@app.get("/entregadores/", response_model=list[Entregador])
def listar_entregadores(session: SessionDep, offset: int = 0, limit: int = 10):
    return session.exec(select(Entregador).offset(offset).limit(limit)).all()

@app.post("/entregadores/", response_model=Entregador)
def criar_entregador(entregador_in: EntregadorCreate, session: SessionDep):
    logger.info(f"Iniciando criação do entregador {entregador_in.nome}")

    try:
        novo_entregador = Entregador(
                nome = entregador_in.nome,
                tipo_veiculo = entregador_in.tipo_veiculo,
                latitude = entregador_in.latitude,
                longitude = entregador_in.longitude
        )
        session.add(novo_entregador)

        # Só salva se der tudo certo 
        session.commit()
        session.refresh(novo_entregador)

        logger.info(f"Entregador {novo_entregador.id_entregador} criado com sucesso!")
        return novo_entregador

    except Exception as e:
        session.rollback() # Desfaz qualquer mudança parcial em caso de erro
        logger.error(f"Erro atômico ao criar entregador: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao processar o entregador") 

@app.post("/entregadores/{id_entregador}/posicao")
def atualizar_posicao(id_entregador: int, posicao: PosicaoUpdate):
    try:
        tabela_telemetria.put_item(
            Item={
                # O deploy.py definiu essa chave como String (S)
                "id_entregador": str(id_entregador), 
                "timestamp": datetime.utcnow().isoformat(),
                # Convertendo para string para evitar problemas de float no DynamoDB
                "latitude": str(posicao.latitude),
                "longitude": str(posicao.longitude)
            }
        )
        return {"status": "Posição recebida"}
    except Exception as e:
        logger.error(f"Erro ao salvar telemetria: {e}")
        raise HTTPException(status_code=500, detail="Erro ao salvar posição")

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

    try:

        novo_pedido = Pedido(
            id_cliente=cliente.id_cliente,
            id_restaurante=restaurante.id_restaurante,
            id_entregador=None,
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
        
        # Só salva se der tudo certo 
        session.commit()
        session.refresh(novo_pedido)
        
        logger.info(f"Pedido {novo_pedido.id_pedido} criado com sucesso!")
        return novo_pedido

    except Exception as e:
        session.rollback() # Desfaz qualquer mudança parcial em caso de erro
        logger.error(f"Erro atômico ao criar pedido: {e}")
        raise HTTPException(status_code=500, detail="Erro interno ao processar o pedido")

@app.get("/pedidos/{id_pedido}/acompanhamento")
def acompanhar_pedido(id_pedido: int): # Removido o SessionDep! A rota agora é 100% NoSQL
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
    # 1. Busca o pedido no RDS
    pedido = session.get(Pedido, id_pedido)
    if not pedido:
        raise HTTPException(status_code=404, detail="Pedido não encontrado")

    if pedido.id_entregador == None:
        logger.warning(f"Tentativa de transição ilegal no Pedido {id_pedido}: o pedido ainda não possui um entregador")
        raise HTTPException(
            status_code=400,
            detail=f"Transição inválida. O pedido ainda não possui entregador"
        )

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
        tabela_eventos.put_item(
            Item={
                "id_pedido": str(id_pedido),
                "timestamp": datetime.utcnow().isoformat(),
                "status": novo_status,
                "event_id": uuid.uuid4().hex[:8],
                "id_entregador": str(pedido.id_entregador)
            }
        )

        logger.info(f"Pedido {id_pedido} avançou para {novo_status}")
        return pedido

    except Exception as e:
        session.rollback()
        logger.error(f"Falha ao processar mudança de status do pedido {id_pedido}: {e}")
        raise HTTPException(status_code=500, detail="Erro interno de comunicação com os bancos de dados.")

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

# Rotas otimizadas para o simulador, as outras faziam o computador morrer
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
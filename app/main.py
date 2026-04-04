import logging
import os
from datetime import datetime
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query
from sqlmodel import Field, Session, SQLModel, create_engine, select

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

class Pedido(SQLModel, table=True):
    __tablename__ = "pedidos"
    id_pedido: int | None = Field(default=None, primary_key=True)
    id_cliente: int = Field(foreign_key="clientes.id_cliente")
    id_restaurante: int = Field(foreign_key="restaurantes.id_restaurante")
    id_entregador: int = Field(foreign_key="entregadores.id_entregador")
    data_criacao: datetime = Field(default_factory=datetime.now)

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
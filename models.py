from sqlalchemy import Column, Integer, Date, Float, Time, String
from sqlalchemy.orm import declarative_base

BASE = declarative_base()

class Silver(BASE):
    __tablename__ = "Silver_Inmet"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cidade = Column(String(20))
    data = Column(Date)
    hora_utc = Column(Time)
    precipitacao_total_mm = Column(Float)
    pressao_estacao_mb = Column(Float)
    pressao_max_ant_mb = Column(Float)
    pressao_min_ant_mb = Column(Float)
    radiacao_global_kj_m2 = Column(Float)
    temp_ar_c = Column(Float)
    temp_orvalho_c = Column(Float)
    temp_max_ant_c = Column(Float)
    temp_min_ant_c = Column(Float)
    temp_orvalho_max_ant_c = Column(Float)
    temp_orvalho_min_ant_c = Column(Float)
    umid_max_ant = Column(Float)
    umid_min_ant = Column(Float)
    umidade_relativa = Column(Float)
    vento_direcao_graus = Column(Float)
    vento_rajada_max_ms = Column(Float)
    vento_velocidade_ms = Column(Float)

class Gold(BASE):
    __tablename__ = "Gold_Inmet"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cidade = Column(String(255))
    data = Column(Date)
    hora_utc = Column(Time)
    precipitacao_total_mm = Column(Float)
    temp_ar_c = Column(Float)
    temp_orvalho_c = Column(Float)
    temp_max_ant_c = Column(Float)
    temp_min_ant_c = Column(Float)
    temp_orvalho_max_ant_c = Column(Float)
    temp_orvalho_min_ant_c = Column(Float)
    umid_max_ant = Column(Float)
    umid_min_ant = Column(Float)
    umidade_relativa = Column(Float)
    vento_direcao_graus = Column(Float)
    vento_rajada_max_ms = Column(Float)
    vento_velocidade_ms = Column(Float)
import os
from sqlalchemy import create_engine, func, text, Column
from sqlalchemy.orm import Session
from models import BASE, Silver, Gold  
from connections import DATABASE_URL

COLUNAS_PARA_NORMALIZAR = [
    "precipitacao_total_mm",
    "temp_ar_c",
    "temp_orvalho_c",
    "temp_max_ant_c",
    "temp_min_ant_c",
    "temp_orvalho_max_ant_c",
    "temp_orvalho_min_ant_c",
    "umid_max_ant",
    "umid_min_ant",
    "umidade_relativa",
    "vento_direcao_graus",
    "vento_rajada_max_ms",
    "vento_velocidade_ms",
]

def normalize(valor, v_min, v_max):
    if valor is None or v_min is None or v_max is None:
        return None
    
    if (v_max - v_min) == 0:
        return 0.0  
    
    return (valor - v_min) / (v_max - v_min)

def main():
    engine = create_engine(DATABASE_URL, future=True)

    with Session(engine) as session:
        
        print("Limpando tabela Gold...")
        try:
            session.execute(text('TRUNCATE TABLE "Gold_Inmet" RESTART IDENTITY;'))
            session.commit()
        except Exception as e:
            print(f"Não foi possível truncar a tabela (pode não existir ainda): {e}")
            session.rollback() 

        print("Buscando dados da tabela Silver...")
        registros_silver = session.query(Silver).all()
        
        if not registros_silver:
            print("Nenhum dado encontrado na tabela Silver. Encerrando.")
            return

        print(f"Encontrados {len(registros_silver)} registros na Silver.")

        print("Calculando valores Min/Max de cada coluna...")
        min_max_valores = {}
        for nome_coluna in COLUNAS_PARA_NORMALIZAR:
            coluna_modelo = getattr(Silver, nome_coluna)
            
            min_val, max_val = session.query(
                func.min(coluna_modelo),
                func.max(coluna_modelo)
            ).one()
            
            min_max_valores[nome_coluna] = {"min": min_val, "max": max_val}


        print("Normalizando dados e criando registros Gold...")
        registros_gold = []
        for silver in registros_silver:
            
            gold = Gold(
                cidade=silver.cidade,
                data=silver.data,
                hora_utc=silver.hora_utc
            )
            
            for nome_coluna in COLUNAS_PARA_NORMALIZAR:
                valor_silver = getattr(silver, nome_coluna)
                v_min = min_max_valores[nome_coluna]["min"]
                v_max = min_max_valores[nome_coluna]["max"]
                
                valor_normalizado = normalize(valor_silver, v_min, v_max)
                
                setattr(gold, nome_coluna, valor_normalizado)
            
            registros_gold.append(gold)

        print(f"Inserindo {len(registros_gold)} registros normalizados na tabela Gold...")
        session.add_all(registros_gold)
        session.commit()

        print("Processo Gold (Normalização Min-Max) finalizado com sucesso.")


if __name__ == "__main__":
    main()
import os
import csv
from datetime import datetime, time
from medalhao.models import Silver
from medalhao.connections import Session

def parse_float(valor):
    if valor is None or valor.strip() == "":
        return None
    return float(valor.replace(",", "."))

def parse_date(v):
    return datetime.strptime(v, "%Y/%m/%d").date()

def parse_hora_utc(v):
    v = v.replace(" UTC", "")
    return time(int(v[:2]), int(v[2:]))

def extract_city_from_filename(nome):
    partes = nome.split("_")
    cidade = partes[4]
    return cidade.capitalize()

def processar_csv(path_csv):
    nome_arquivo = os.path.basename(path_csv)
    cidade = extract_city_from_filename(nome_arquivo)

    with open(path_csv, "r", encoding="latin-1") as f:
        reader = csv.reader(f, delimiter=";")

        header = next(reader)

        registros = []
        for row in reader:
            try:
                data = parse_date(row[0])
                hora = parse_hora_utc(row[1])

                registro = Silver(
                    cidade=cidade,
                    data=data,
                    hora_utc=hora,
                    precipitacao_total_mm=parse_float(row[2]),
                    pressao_estacao_mb=parse_float(row[3]),
                    pressao_max_ant_mb=parse_float(row[4]),
                    pressao_min_ant_mb=parse_float(row[5]),
                    radiacao_global_kj_m2=parse_float(row[6]),
                    temp_ar_c=parse_float(row[7]),
                    temp_orvalho_c=parse_float(row[8]),
                    temp_max_ant_c=parse_float(row[9]),
                    temp_min_ant_c=parse_float(row[10]),
                    temp_orvalho_max_ant_c=parse_float(row[11]),
                    temp_orvalho_min_ant_c=parse_float(row[12]),
                    umid_max_ant=parse_float(row[13]),
                    umid_min_ant=parse_float(row[14]),
                    umidade_relativa=parse_float(row[15]),
                    vento_direcao_graus=parse_float(row[16]),
                    vento_rajada_max_ms=parse_float(row[17]),
                    vento_velocidade_ms=parse_float(row[18]),
                )
                registros.append(registro)

            except Exception as e:
                print(f"Erro ao processar linha em {nome_arquivo}: {row}")
                print("Erro:", e)

    return registros

from sqlalchemy import text

def main():
    print("Iniciando o processamento dos dados Silver…")
    pasta = "data/inmet_etl_bronze"

    arquivos = [
        os.path.join(pasta, f)
        for f in os.listdir(pasta)
        if f.lower().endswith(".csv")
    ]

    print(f"Encontrados {len(arquivos)} CSVs.")

    total_linhas = 0
    with Session() as session:
        session.execute(text('TRUNCATE TABLE "Silver_Inmet" RESTART IDENTITY;'))
        session.commit()

        for csv_file in arquivos:
            print(f"Processando: {csv_file}")
            registros = processar_csv(csv_file)
            print(f"Inserindo {len(registros)} linhas…")
            total_linhas += len(registros)
            session.add_all(registros)
            session.commit()

    print("Finalizado.")
    print(f"Total de {total_linhas} linhas.")


if __name__ == "__main__":
    main()
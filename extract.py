import os
from pathlib import Path
import requests
import zipfile

BASE_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos"

DESTINO_BASE = Path("data/inmet_raw")


def baixar_arquivo_ano(ano: int) -> Path:
    """Baixa o ZIP do INMET para um ano específico."""
    DESTINO_BASE.mkdir(parents=True, exist_ok=True)

    url = f"{BASE_URL}/{ano}.zip"
    destino_zip = DESTINO_BASE / f"{ano}.zip"

    if destino_zip.exists():
        print(f"[OK] {destino_zip} já existe. Pulando download.")
        return destino_zip

    print(f"[↓] Baixando {url} ...")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    destino_zip.write_bytes(resp.content)

    print(f"[✔] Download concluído: {destino_zip}")
    return destino_zip


def extrair_zip(zip_path: Path):
    """Extrai o ZIP e apaga o arquivo compactado."""
    ano = zip_path.stem
    pasta_destino = DESTINO_BASE / ano
    pasta_destino.mkdir(exist_ok=True)

    print(f"[→] Extraindo {zip_path} para {pasta_destino} ...")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(pasta_destino)

    print(f"[✔] Extração concluída!")

    print(f"[✂] Removendo ZIP {zip_path} ...")
    zip_path.unlink()


def processar_anos(anos):
    for ano in anos:
        try:
            zip_file = baixar_arquivo_ano(ano)
            extrair_zip(zip_file)
        except Exception as e:
            print(f"[ERRO] Ano {ano}: {e}")


if __name__ == "__main__":
    anos = [2020, 2021, 2022, 2023, 2024]
    processar_anos(anos)
    print("\n[✔] Finalizado com sucesso!")
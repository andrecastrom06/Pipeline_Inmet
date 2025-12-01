from pathlib import Path

PASTA_ORIGINAL = Path("../data/inmet_raw")
PASTA_LIMPA = Path("../data/inmet_etl_bronze")
PASTA_LIMPA.mkdir(parents=True, exist_ok=True)

def corrigir_acentos_e_limpar(caminho: Path):
    with open(caminho, "r", encoding="latin-1", errors="ignore") as f:
        linhas = f.readlines()

    linhas = linhas[8:]

    destino = PASTA_LIMPA / caminho.name

    with open(destino, "w", encoding="utf-8") as f:
        f.writelines(linhas)

    print(f"[OK] Corrigido e salvo: {destino}")


def processar_todos():
    arquivos = list(PASTA_ORIGINAL.rglob("INMET_NE_PE_*.CSV"))
    if not arquivos:
        print("[AVISO] Nenhum arquivo encontrado.")
        return

    for arq in arquivos:
        try:
            corrigir_acentos_e_limpar(arq)
        except Exception as e:
            print(f"[ERRO] {arq}: {e}")


if __name__ == "__main__":
    processar_todos()
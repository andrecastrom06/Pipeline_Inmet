import traceback
from medalhao.connections import engine
from medalhao.models import BASE
import sys

try:
    from medalhao.extract import main as extract_main
except ImportError:
    print("Erro: Não foi possível encontrar o arquivo 'extract.py'.")
    sys.exit(1)

try:
    from medalhao.etl_bronze import main as bronze_main
except ImportError:
    print("Erro: Não foi possível encontrar o arquivo 'etl_bronze.py'.")
    sys.exit(1)

try:
    from medalhao.etl_silver import main as silver_main
except ImportError:
    print("Erro: Não foi possível encontrar o arquivo 'etl_silver.py'.")
    sys.exit(1)

try:
    from medalhao.etl_gold import main as gold_main
except ImportError:
    print("Erro: Não foi possível encontrar o arquivo 'etl_gold.py'.")
    print("Verifique se o script gold que criei para você está salvo com este nome.")
    sys.exit(1)

try:
    from medalhao.ml_train import main as ml_main
except ImportError:
    print("Erro: Não foi possível encontrar o arquivo 'ml_train.py'.")
    print("Verifique se o script ml_train que criei para você está salvo com este nome.")
    sys.exit(1)

def main():
    print(" Iniciando pipeline ETL completo (Bronze -> Silver -> Gold)...")
    
    try:
        BASE.metadata.create_all(engine)
        print("  [OK] Tabelas (Silver_Inmet, Gold_Inmet) verificadas/criadas.")
    except Exception as e:
        print(f"\n  [ERRO CRÍTICO] Erro ao conectar/criar tabelas no banco: {e}")
        traceback.print_exc()
        return
    
    print("\n---  ETAPA DE EXTRAÇÃO (Extração de arquivos crus via API Inmet) ---")
    try:
        extract_main()
    except Exception as e:
        print(f"\n  [ERRO CRÍTICO] Falha na etapa EXTRAÇÃO: {e}")
        print("   O pipeline foi interrompido.")
        return

    print("\n---  ETAPA BRONZE (Limpeza inicial para csv) ---")
    try:
        bronze_main()
    except Exception as e:
        print(f"\n  [ERRO CRÍTICO] Falha na etapa BRONZE: {e}")
        print("   O pipeline foi interrompido.")
        return

    print("\n---  ETAPA SILVER (CSV para Tabela) ---")
    try:
        silver_main()
    except Exception as e:
        print(f"\n  [ERRO CRÍTICO] Falha na etapa SILVER: {e}")
        print("   O pipeline foi interrompido.")
        return

    print("\n---  ETAPA GOLD (Normalização Min-Max) ---")
    try:
        gold_main()
    except Exception as e:
        print(f"\n  [ERRO CRÍTICO] Falha na etapa GOLD: {e}")
        print(f"   O pipeline foi interrompido.")
        return
    
    try:
        ml_main()
    except Exception as e:
        print(f"\n  [ERRO CRÍTICO] Falha na etapa de Machine Learning: {e}")
        print(f"   O pipeline foi interrompido.")
        return

    print(f"\n [SUCESSO] Pipeline ETL completo finalizado.")


if __name__ == "__main__":
    main()
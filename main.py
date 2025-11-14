from connections import engine
from models import BASE
from etl_silver import main as silver

def main():
    print("🔹 Iniciando extração e persistência de dados no banco...")
    
    try:
        BASE.metadata.create_all(engine)
        print("\n✅ Todas as tabelas criadas com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao verificar/criar tabelas no banco: {e}")
        return 
    try:
        print("\n---ETL SILVER---")
        silver()
    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO DURANTE A SINCRONIZAÇÃO: {e}")
        print("   O processo foi interrompido.")
    finally:
        print(f"\n✅ Processo finalizado")

if __name__ == "__main__":
    main()
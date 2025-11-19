from connections import Session       
from models import Gold, Cluster
import pandas as pd
from sqlalchemy.orm import Session as SASessionType
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
import mlflow
import mlflow.sklearn


# -------------------------------
# 1. Carregar dados da tabela Gold
# -------------------------------
def carregar_gold():
    session: SASessionType = Session()

    query = session.query(Gold)
    df = pd.read_sql(query.statement, session.bind)

    session.close()
    return df


# -------------------------------
# 2. Preparar dados agrupando por cidade
# -------------------------------
def preparar_dados(df):
    df_grouped = df.groupby("cidade").agg({
        "temp_ar_c": "mean",
        "precipitacao_total_mm": "mean",
        "umidade_relativa": "mean",
        "vento_velocidade_ms": "mean"
    }).reset_index()

    return df_grouped


# -------------------------------
# 3. Treinar modelo KMeans
# -------------------------------
def treinar_modelo(df_grouped, n_clusters=4):

    features = df_grouped[[
        "temp_ar_c",
        "precipitacao_total_mm",
        "umidade_relativa",
        "vento_velocidade_ms"
    ]]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)

    mlflow.set_experiment("Cluster_Estacoes_Meteorologicas")

    with mlflow.start_run():
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        labels = kmeans.fit_predict(X_scaled)

        # Log params
        mlflow.log_param("n_clusters", n_clusters)

        # Log metric
        mlflow.log_metric("inertia", kmeans.inertia_)

        # Log models
        mlflow.sklearn.log_model(kmeans, "modelo_kmeans")
        mlflow.sklearn.log_model(scaler, "scaler")

    df_grouped["cluster"] = labels

    return df_grouped


# -------------------------------
# 4. Tabela final com características dos clusters
# -------------------------------
def gerar_tabela_clusters(df_clusterizado):
    tabela = df_clusterizado.groupby("cluster").agg({
        "temp_ar_c": "mean",
        "precipitacao_total_mm": "mean",
        "umidade_relativa": "mean",
        "vento_velocidade_ms": "mean"
    })

    print(tabela)
    return tabela

def salvar_clusters_no_banco(df_clusters):
    session = Session()

    # Limpa a tabela antes de inserir (opcional)
    session.query(Cluster).delete()

    for _, row in df_clusters.iterrows():
        registro = Cluster(
            cidade=row["cidade"],
            Cluster=int(row["cluster"])
        )
        session.add(registro)

    session.commit()
    session.close()
    print("Clusters salvos no banco com sucesso!")

# -------------------------------
# 5. Main
# -------------------------------
def main():
    df = carregar_gold()
    df_prep = preparar_dados(df)
    df_clusters = treinar_modelo(df_prep, n_clusters=4)
    tabela = gerar_tabela_clusters(df_clusters)

    print("\nClusterização final por cidade:")
    print(df_clusters[["cidade", "cluster"]])

    salvar_clusters_no_banco(df_clusters)


if __name__ == "__main__":
    main()
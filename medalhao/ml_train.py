from medalhao.connections import Session       
from medalhao.models import Gold, Cluster
import pandas as pd
from sqlalchemy.orm import Session as SASessionType
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
import mlflow
import mlflow.sklearn
from sklearn.metrics import silhouette_score
import numpy as np
from sqlalchemy import text

def encontrar_k_otimo(X_scaled, max_k=10):
    n_samples = X_scaled.shape[0]

    max_k = min(max_k, n_samples - 1)
    if max_k < 2:
        raise ValueError("Número insuficiente de cidades para clustering.")

    inercia = []
    silhuetas = []
    Ks = range(2, max_k + 1)

    for k in Ks:
        kmeans = KMeans(n_clusters=k, random_state=42)
        labels = kmeans.fit_predict(X_scaled)

        inercia.append(kmeans.inertia_)

        if len(set(labels)) >= 2 and len(set(labels)) <= n_samples - 1:
            silhuetas.append(silhouette_score(X_scaled, labels))
        else:
            silhuetas.append(-1)

    k_silhueta = Ks[np.argmax(silhuetas)]
    reducoes = np.diff(inercia)
    k_elbow = Ks[np.argmin(reducoes)]

    mlflow.log_param("k_silhueta", k_silhueta)
    mlflow.log_param("k_elbow", k_elbow)

    k_final = int(round((k_silhueta + k_elbow) / 2))

    print(f"K sugerido pela silhueta: {k_silhueta}")
    print(f"K sugerido pelo cotovelo: {k_elbow}")
    print(f"K final escolhido: {k_final}")

    return k_final

def treinar_modelo(df_grouped):

    features = df_grouped[[ 
        "temp_ar_c",
        "precipitacao_total_mm",
        "umidade_relativa",
        "vento_velocidade_ms"
    ]]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)
    X_scaled_df = pd.DataFrame(X_scaled, columns=features.columns)

    mlflow.set_tracking_uri("sqlite:///mlutils/mlflow.db")
    mlflow.set_experiment("Cluster_Estacoes_Meteorologicas")

    with mlflow.start_run():

        k_otimo = encontrar_k_otimo(X_scaled_df.values)

        while True:
            kmeans = KMeans(n_clusters=k_otimo, random_state=42)
            labels = kmeans.fit_predict(X_scaled_df)

            if len(set(labels)) == k_otimo and \
               all(list(labels).count(c) > 1 for c in set(labels)):
                break

            k_otimo -= 1
            if k_otimo < 2:
                k_otimo = 2
                break

        mlflow.log_param("k_clusters_final", k_otimo)
        mlflow.log_metric("inertia_final", kmeans.inertia_)

        pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("to_array", FunctionTransformer(lambda x: np.asarray(x), validate=False)),
            ("kmeans", KMeans(n_clusters=k_otimo, random_state=42))
        ])

        input_example = df_grouped[[ 
            "temp_ar_c",
            "precipitacao_total_mm",
            "umidade_relativa",
            "vento_velocidade_ms"
        ]].head(1).astype("float64")

        mlflow.sklearn.log_model(
            pipeline,
            name="modelo_clusters",
            input_example=input_example
        )

    df_grouped["cluster"] = labels
    return df_grouped

def carregar_gold():
    session: SASessionType = Session()
    query = session.query(Gold)
    df = pd.read_sql(query.statement, session.bind)
    session.close()
    return df

def preparar_dados(df):
    df_grouped = df.groupby("cidade").agg({
        "temp_ar_c": "mean",
        "precipitacao_total_mm": "mean",
        "umidade_relativa": "mean",
        "vento_velocidade_ms": "mean"
    }).reset_index()
    return df_grouped

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

    session.execute(text('TRUNCATE TABLE "Cidades_Cluster" RESTART IDENTITY CASCADE;'))

    for _, row in df_clusters.iterrows():
        registro = Cluster(
            cidade=row["cidade"],
            cluster=int(row["cluster"])
        )
        session.add(registro)

    session.commit()
    session.close()
    print("Clusters salvos no banco com sucesso!")

def main():
    print("Iniciando o processo de clusterização...")
    df = carregar_gold()
    print("Dados carregados da tabela Gold.")

    df_prep = preparar_dados(df)
    print("Dados preparados e agrupados por cidade.")

    print("\nTreinando modelo de clusterização...")
    df_clusterizado = treinar_modelo(df_prep)

    print("\nNúmero total de clusters:", df_clusterizado["cluster"].nunique())

    print("\nCidades por cluster:")
    for cluster_id, grupo in df_clusterizado.groupby("cluster"):
        cidades = grupo["cidade"].tolist()
        print(f"\nCluster {cluster_id} ({len(cidades)} cidades):")
        for cidade in cidades:
            print(f" - {cidade}")

    salvar_clusters_no_banco(df_clusterizado)
    return df_clusterizado

if __name__ == "__main__":
    main()
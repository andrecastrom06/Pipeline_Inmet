# Pipeline_Inmet
## Análise e Visualização de Dados - 2025.2
## CESAR School

## 🫂Equipe🫂
* André Castro - andrecastrom06
* Caio Lima - Clb7cc
* Felipe Caminha - Fcc2187
* José Braz - jbraz05
* Lucas Sukar - LucasSukar
* Miguel Becker - Becker1406
* Rodrigo Torres - rtmr01

## Como Rodar
- Constrói as imagens
    ```bash
    docker-compose build
    ```
- Sobe os serviços
    ```bash
    docker-compose up
    ```
- Rode em um outro terminal
    ```bash
    docker-compose up -d
    ```
- Acompanhe os logs
    ```bash
    docker-compose logs -f app
    ```
- Para todos os containers
    ```bash
    docker-compose down
    ```

- Para iniciar o MLFlow
    ```bash
    python -m mlflow ui --port 5000
    ```

- Para acessar URL do MLFlow
    ```bash
    http://127.0.0.1:5000
    ```

- Após isso o dashboard estará atualizado e você só deve abrir o arquivo .pbix da pasta trendz/
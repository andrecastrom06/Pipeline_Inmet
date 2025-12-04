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
    
- Para parar todos os containers
    ```bash
    docker-compose down
    ```

- Para iniciar o visual do MLFlow
    ```bash
    python -m mlflow ui --port 5000
    ```

- Para acessar URL do MLFlow
    ```bash
    http://127.0.0.1:5000
    ```

- Após isso o dashboard estará atualizado e você só deve abrir o arquivo .pbix da pasta trendz/

## Especificações do Cronjob

Configuração de nome e descrição
![708f3ebb-6fec-49f5-9fa6-dc002bb60031](https://github.com/user-attachments/assets/6b3f68e5-00fc-4973-b68b-c2c17b1db244)
<br>
Configuração de quando o script será executado automaticamente
![18a4ce80-4ce8-43d9-9d44-eb6b50a4eb7d](https://github.com/user-attachments/assets/b0e32bd3-79e1-46d6-8d42-54db91d4126e)
<br>
Configuração dos scripts que serão atualizados pelo cronjob
![8506497f-d692-45f4-9a37-114e9fa6ca65](https://github.com/user-attachments/assets/c51e6a2d-1455-400f-a4c0-54cd696eb6ef)
<br>
Cronjob rodou com sucesso!
![66b2829a-6cb8-4f89-a577-f5e3bfd43944](https://github.com/user-attachments/assets/533ea25a-a4ba-4bf6-b126-10c35ff455c1)

# ETL_Data_KPI

## Instalar y Ejecutar:
sudo apt install -y docker-compose-v2
newgrp docker
sudo usermod -aG docker $USER

## Generar en el directorio raiz del proyecto el archivo .env

## Generar las carpetas necesarias
mkdir -p ./dags
mkdir -p ./logs
mkdir -p ./plugins
mkdir -p ./data
mkdir -p ./config  

## Primera vez - inicializa todo
docker-compose up airflow-init

## Levanta los servicios
docker-compose up -d

## Verificar que todo esta corriendo
docker-compose ps

## Webpage Airflow API-Server
- ipadd:8080

## Market DB
- Host name/address: market-postgre
- Port: 5432
- Maintenance database: market_data
- Username: ...
- Password: ...
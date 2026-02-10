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
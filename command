docker-compose exec webserver bash

airflow users create \
  --username airflow \
  --password airflow \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email airflow@example.com

  exit

  docker-compose restart webserver scheduler
deploy-prod:
    docker compose build
    docker compose up airflow-init --remove-orphans
    docker compose up -d

logs-scheduler:
    docker compose logs -f airflow-scheduler

down:
    docker compose down

reset-all:
    docker compose down --volumes --remove-orphans
    @echo "Environment and all persistent database volumes have been wiped."

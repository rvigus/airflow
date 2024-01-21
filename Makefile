up:
	@echo "Starting Airflow and DWH"
	docker-compose up -d && astro dev start

down:
	@echo "Stopping Airflow and DWH"
	docker-compose down && astro dev stop

db-up:
	@echo "Starting DWH"
	docker-compose up -d

db-down:
	@echo "Stopping DWH"
	docker-compose down

airflow-up:
	@echo "Starting airflow"
	astro dev start

airflow-down:
	@echo "Stopping airflow"
	astro dev stop

airflow-restart:
	@echo "Restarting airflow"
	astro dev restart

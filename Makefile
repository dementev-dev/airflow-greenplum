SHELL := /bin/bash
UV := uv
PYTHON_VERSION := 3.11
DEMODB_REPO := https://github.com/postgrespro/demodb.git
DEMODB_COMMIT := d68de192850237719f09b47688d5f3fc94653ca6
BOOKINGS_JOBS ?= 1
BOOKINGS_START_DATE ?= 2017-01-01
BOOKINGS_INIT_DAYS ?= 1

.PHONY: up down airflow-init logs gp-psql ddl-gp \
	bookings-clone-demodb bookings-init bookings-psql bookings-generate-day \
	dev-setup dev-sync dev-lock test lint fmt clean-venv

up:
	docker compose -f docker-compose.yml up -d

down:
	docker compose -f docker-compose.yml down -v

airflow-init:
	docker compose -f docker-compose.yml run --rm airflow-init

logs:
	docker compose -f docker-compose.yml logs -f airflow-webserver airflow-scheduler

gp-psql:
	docker compose -f docker-compose.yml exec greenplum bash -c "su - gpadmin -c '/usr/local/greenplum-db/bin/psql -p 5432 -d gp_dwh'"

ddl-gp:
	docker compose -f docker-compose.yml exec greenplum bash -c "su - gpadmin -c '/usr/local/greenplum-db/bin/psql -d gp_dwh -f /sql/ddl_gp.sql'"

bookings-clone-demodb:
	mkdir -p bookings
	if [ ! -d bookings/demodb ]; then \
		git clone --depth 1 $(DEMODB_REPO) bookings/demodb; \
		git -C bookings/demodb fetch --depth 1 origin $(DEMODB_COMMIT); \
		git -C bookings/demodb checkout $(DEMODB_COMMIT); \
	fi
	# Патчим generate/continue: при jobs=1 запускаем process_queue синхронно, без dblink
	if ! grep -q "CALL process_queue(end_date);" bookings/demodb/engine.sql; then \
		patch -d bookings/demodb -p1 --forward < bookings/patches/engine_jobs1_sync.patch || true; \
	fi
	# Делаем установку идемпотентной: DROP DATABASE IF EXISTS demo
	if ! grep -q "DROP DATABASE IF EXISTS demo;" bookings/demodb/install.sql; then \
		patch -d bookings/demodb -p1 --forward < bookings/patches/install_drop_if_exists.patch || true; \
	fi

bookings-init: bookings-clone-demodb
	docker compose -f docker-compose.yml up -d bookings-db
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		until PGPASSWORD="$$POSTGRES_PASSWORD" pg_isready -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -h localhost; do \
			echo "Waiting for bookings-db to become ready..."; \
			sleep 1; \
		done \
	'
	docker compose -f docker-compose.yml exec bookings-db bash -lc 'cd /bookings/demodb && PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -f install.sql'
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		CONNSTR="dbname=demo user=$$POSTGRES_USER password=$$POSTGRES_PASSWORD"; \
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "ALTER DATABASE demo SET gen.connstr='\''$$CONNSTR'\'';" \
	'
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d demo -c "\
			ALTER DATABASE demo SET bookings.start_date = '\''$(BOOKINGS_START_DATE)'\''; \
			ALTER DATABASE demo SET bookings.init_days = '\''$(BOOKINGS_INIT_DAYS)'\''; \
			ALTER DATABASE demo SET bookings.jobs = '\''$(BOOKINGS_JOBS)'\'';" \
	'
	# Генерируем первый день данных, чтобы база не оставалась пустой
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d demo -f /bookings/generate_next_day.sql \
	'

bookings-psql:
	docker compose -f docker-compose.yml exec bookings-db bash -lc 'PGPASSWORD="$$POSTGRES_PASSWORD" psql -U "$$POSTGRES_USER" -d demo'

bookings-generate-day:
	docker compose -f docker-compose.yml up -d bookings-db
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		until PGPASSWORD="$$POSTGRES_PASSWORD" pg_isready -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -h localhost; do \
			echo "Waiting for bookings-db to become ready..."; \
			sleep 1; \
		done \
	'
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d demo -f /bookings/generate_next_day.sql \
	'

dev-setup:
	$(UV) python install $(PYTHON_VERSION)
	$(UV) python pin $(PYTHON_VERSION)
	$(UV) sync

dev-sync:
	$(UV) sync

dev-lock:
	$(UV) lock --upgrade

test:
	$(UV) run pytest -q

lint:
	$(UV) run black --check airflow tests
	$(UV) run isort --check-only airflow tests

fmt:
	$(UV) run black airflow tests
	$(UV) run isort airflow tests

clean-venv:
	python -c "import shutil; shutil.rmtree('.venv', ignore_errors=True)"

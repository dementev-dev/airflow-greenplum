SHELL := /bin/bash
UV := uv
PYTHON_VERSION := 3.11
DEMODB_REPO := https://github.com/postgrespro/demodb.git
DEMODB_COMMIT := d68de192850237719f09b47688d5f3fc94653ca6
BOOKINGS_JOBS ?= 1
BOOKINGS_START_DATE ?= 2017-01-01
BOOKINGS_INIT_DAYS ?= 1

.PHONY: up stop down clean airflow-init logs gp-psql ddl-gp \
	bookings-clone-demodb bookings-init bookings-psql bookings-generate-day \
	dev-setup dev-sync dev-lock test lint fmt clean-venv build
SHELL := /bin/bash

up:
	docker compose -f docker-compose.yml up -d

build:
	docker compose -f docker-compose.yml build

stop:
	docker compose -f docker-compose.yml stop

down:
	docker compose -f docker-compose.yml down

clean:
	docker compose -f docker-compose.yml down -v

airflow-init:
	docker compose -f docker-compose.yml run --rm airflow-init

logs:
	docker compose -f docker-compose.yml logs -f airflow-webserver airflow-scheduler

gp-psql:
	docker compose -f docker-compose.yml exec greenplum bash -c "su - gpadmin -c '/usr/local/greenplum-db/bin/psql -p 5432 -d gp_dwh'"

ddl-gp:
	docker compose -f docker-compose.yml exec greenplum bash -c "su - gpadmin -c 'cd /sql && /usr/local/greenplum-db/bin/psql -d gp_dwh -f ddl_gp.sql'"

bookings-clone-demodb:
	mkdir -p bookings
	if [ ! -d bookings/demodb ]; then \
		git clone --depth 1 $(DEMODB_REPO) bookings/demodb; \
		git -C bookings/demodb fetch --depth 1 origin $(DEMODB_COMMIT); \
		git -C bookings/demodb checkout $(DEMODB_COMMIT); \
	fi
	# Патчим generate/continue: при jobs=1 запускаем process_queue синхронно, без dblink
	if ! grep -q "Job 1 (local): ok" bookings/demodb/engine.sql; then \
		if ! patch -d bookings/demodb -p1 --forward < bookings/patches/engine_jobs1_sync.patch; then \
			echo "Не удалось применить патч engine_jobs1_sync.patch. Удалите bookings/demodb и повторите make bookings-init." >&2; \
			exit 1; \
		fi; \
	fi
	# Делаем установку идемпотентной и принудительной: DROP DATABASE IF EXISTS demo WITH (FORCE)
	if ! grep -q "DROP DATABASE IF EXISTS demo WITH (FORCE);" bookings/demodb/install.sql; then \
		if ! patch -d bookings/demodb -p1 --forward < bookings/patches/install_drop_if_exists.patch; then \
			echo "Не удалось применить патч install_drop_if_exists.patch. Удалите bookings/demodb и повторите make bookings-init." >&2; \
			exit 1; \
		fi; \
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
	# Проверяем и загружаем reference-данные, если таблицы пустые (иногда \copy в install.sql не срабатывает)
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d demo -c "\
			DO \$$ \
			BEGIN \
				IF (SELECT COUNT(*) FROM gen.airports_data) = 0 THEN \
					COPY gen.airports_data FROM '\''/bookings/demodb/airports_data.dat'\''; \
					COPY gen.airplanes_data FROM '\''/bookings/demodb/airplanes_data.dat'\''; \
					COPY gen.seats FROM '\''/bookings/demodb/seats.dat'\''; \
					COPY gen.firstnames FROM '\''/bookings/demodb/firstnames.dat'\''; \
					COPY gen.lastnames FROM '\''/bookings/demodb/lastnames.dat'\''; \
				END IF; \
			END \$$;" \
	'
	# Заполняем cume_dist для firstnames/lastnames (иначе get_passenger_name зависнет в бесконечном цикле)
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d demo -c "\
			UPDATE gen.firstnames SET cume_dist = sub.cumedist FROM ( \
				SELECT country, grp, name, cume_dist() OVER (PARTITION BY country ORDER BY qty) as cumedist \
				FROM gen.firstnames \
			) sub WHERE gen.firstnames.country = sub.country AND gen.firstnames.grp = sub.grp AND gen.firstnames.name = sub.name; \
			UPDATE gen.lastnames SET cume_dist = sub.cumedist FROM ( \
				SELECT country, grp, name, cume_dist() OVER (PARTITION BY country, grp ORDER BY qty) as cumedist \
				FROM gen.lastnames \
			) sub WHERE gen.lastnames.country = sub.country AND gen.lastnames.grp = sub.grp AND gen.lastnames.name = sub.name; \
			UPDATE gen.airport_to_prob SET cume_dist = sub.cumedist FROM ( \
				SELECT departure_airport, arrival_airport, domestic, cume_dist() OVER (PARTITION BY departure_airport, domestic ORDER BY prob) as cumedist \
				FROM gen.airport_to_prob \
			) sub WHERE gen.airport_to_prob.departure_airport = sub.departure_airport AND gen.airport_to_prob.arrival_airport = sub.arrival_airport AND (gen.airport_to_prob.domestic = sub.domestic OR gen.airport_to_prob.domestic IS NULL);" \
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

e2e-smoke:
	./scripts/e2e_smoke.sh

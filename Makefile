SHELL := /bin/bash
UV := uv
PYTHON_VERSION := 3.11
DEMODB_REPO := https://github.com/postgrespro/demodb.git
DEMODB_COMMIT := 866e56f7fe54596a1d2a88f5f32f4aa3b2698121
BOOKINGS_JOBS ?= 2
BOOKINGS_START_DATE ?= 2017-01-01
BOOKINGS_INIT_DAYS ?= 60

.PHONY: up stop down clean airflow-init logs gp-psql ddl-gp \
	bookings-check-jobs bookings-clone-demodb bookings-init bookings-generate bookings-dump \
	bookings-psql bookings-generate-day \
	dev-setup dev-sync dev-lock test lint fmt clean-venv build e2e-smoke e2e-etl
BOOKINGS_SEED := bookings/seed/demo.sql.xz
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

dwh-truncate:
	docker compose -f docker-compose.yml exec greenplum bash -c "su - gpadmin -c 'cd /sql && /usr/local/greenplum-db/bin/psql -d gp_dwh -f truncate_gp.sql'"

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
			echo "Не удалось применить патч engine_jobs1_sync.patch. Удалите bookings/demodb и повторите make bookings-generate." >&2; \
			exit 1; \
		fi; \
	fi
	# Делаем установку идемпотентной и принудительной: DROP DATABASE IF EXISTS demo WITH (FORCE)
	if ! grep -q "DROP DATABASE IF EXISTS demo WITH (FORCE);" bookings/demodb/install.sql; then \
		if ! patch -d bookings/demodb -p1 --forward < bookings/patches/install_drop_if_exists.patch; then \
			echo "Не удалось применить патч install_drop_if_exists.patch. Удалите bookings/demodb и повторите make bookings-generate." >&2; \
			exit 1; \
		fi; \
	fi
	# Убираем хардкод gen.connstr без credentials — иначе VACUUM падает
	if grep -q "gen.connstr = 'dbname=demo'" bookings/demodb/install.sql; then \
		if ! patch -d bookings/demodb -p1 --forward < bookings/patches/install_connstr_no_hardcode.patch; then \
			echo "Не удалось применить патч install_connstr_no_hardcode.patch. Удалите bookings/demodb и повторите make bookings-generate." >&2; \
			exit 1; \
		fi; \
	fi

bookings-check-jobs:
	@case "$(BOOKINGS_JOBS)" in \
		''|*[!0-9]*) echo "BOOKINGS_JOBS должен быть целым числом >= 1. Текущее значение: '$(BOOKINGS_JOBS)'." >&2; exit 1;; \
	esac; \
	if [ "$(BOOKINGS_JOBS)" -lt 1 ]; then \
		echo "BOOKINGS_JOBS должен быть >= 1. Текущее значение: $(BOOKINGS_JOBS)." >&2; \
		exit 1; \
	fi

bookings-generate: bookings-check-jobs bookings-clone-demodb
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
	# Генерируем данные за $(BOOKINGS_INIT_DAYS) дней
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d demo -f /bookings/generate_next_day.sql \
	'
	# Валидация: ключевые таблицы не должны быть пустыми
	@docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d demo -tAc " \
			SELECT format(E'\''%-25s %s'\'', t, cnt) \
			FROM ( \
				SELECT '\''bookings.bookings'\'' AS t, count(*) AS cnt FROM bookings.bookings \
				UNION ALL \
				SELECT '\''bookings.tickets'\'', count(*) FROM bookings.tickets \
				UNION ALL \
				SELECT '\''bookings.flights'\'', count(*) FROM bookings.flights \
				UNION ALL \
				SELECT '\''bookings.boarding_passes'\'', count(*) FROM bookings.boarding_passes \
			) s ORDER BY t; \
		" \
	'
	@docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d demo -tAc " \
			DO \$$\$$ \
			DECLARE v_cnt bigint; \
			BEGIN \
				SELECT count(*) INTO v_cnt FROM bookings.bookings; \
				IF v_cnt = 0 THEN \
					RAISE EXCEPTION '\''bookings.bookings пуста — генерация не сработала. Проверьте логи: SELECT * FROM gen.log ORDER BY at DESC LIMIT 10;'\''; \
				END IF; \
				SELECT count(*) INTO v_cnt FROM bookings.flights; \
				IF v_cnt = 0 THEN \
					RAISE EXCEPTION '\''bookings.flights пуста — попробуйте увеличить BOOKINGS_INIT_DAYS.'\''; \
				END IF; \
			END \$$\$$; \
		" \
	'

bookings-init: bookings-check-jobs
	@if [ ! -f $(BOOKINGS_SEED) ]; then \
		echo "Файл $(BOOKINGS_SEED) не найден. Используйте make bookings-generate для генерации с нуля." >&2; \
		exit 1; \
	fi
	docker compose -f docker-compose.yml up -d bookings-db
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		until PGPASSWORD="$$POSTGRES_PASSWORD" pg_isready -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -h localhost; do \
			echo "Waiting for bookings-db to become ready..."; \
			sleep 1; \
		done \
	'
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" \
			-c "DROP DATABASE IF EXISTS demo WITH (FORCE);" \
	'
	@echo "Восстановление из дампа (~42 MB)..."
	xz -dc $(BOOKINGS_SEED) | docker compose -f docker-compose.yml exec -T bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" \
	'
	# Применяем настройки из текущего окружения поверх дампа
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		CONNSTR="dbname=demo user=$$POSTGRES_USER password=$$POSTGRES_PASSWORD"; \
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -U "$$POSTGRES_USER" -d "$$POSTGRES_DB" -c "\
			ALTER DATABASE demo SET gen.connstr='\''$$CONNSTR'\''; \
			ALTER DATABASE demo SET bookings.start_date = '\''$(BOOKINGS_START_DATE)'\''; \
			ALTER DATABASE demo SET bookings.init_days = '\''$(BOOKINGS_INIT_DAYS)'\''; \
			ALTER DATABASE demo SET bookings.jobs = '\''$(BOOKINGS_JOBS)'\'';" \
	'
	@echo "Восстановление завершено. Проверяем данные..."
	@docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" psql -U "$$POSTGRES_USER" -d demo -tAc " \
			SELECT format(E'\''%-25s %s'\'', t, cnt) \
			FROM ( \
				SELECT '\''bookings.bookings'\'' AS t, count(*) AS cnt FROM bookings.bookings \
				UNION ALL \
				SELECT '\''bookings.tickets'\'', count(*) FROM bookings.tickets \
				UNION ALL \
				SELECT '\''bookings.flights'\'', count(*) FROM bookings.flights \
				UNION ALL \
				SELECT '\''bookings.boarding_passes'\'', count(*) FROM bookings.boarding_passes \
			) s ORDER BY t; \
		" \
	'

bookings-dump:
	@echo "Создание дампа demo → $(BOOKINGS_SEED)..."
	mkdir -p bookings/seed
	docker compose -f docker-compose.yml exec bookings-db bash -lc '\
		PGPASSWORD="$$POSTGRES_PASSWORD" pg_dump -U "$$POSTGRES_USER" -d demo --format=plain --create \
	' > /tmp/demo_dump_$$$$.sql
	xz -9 < /tmp/demo_dump_$$$$.sql > $(BOOKINGS_SEED)
	rm -f /tmp/demo_dump_$$$$.sql
	@echo "Дамп сохранён: $$(ls -lh $(BOOKINGS_SEED) | awk '{print $$5}')"

bookings-psql:
	docker compose -f docker-compose.yml exec bookings-db bash -lc 'PGPASSWORD="$$POSTGRES_PASSWORD" psql -U "$$POSTGRES_USER" -d demo'

bookings-generate-day: bookings-check-jobs
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

e2e-etl:
	./scripts/e2e_etl.sh


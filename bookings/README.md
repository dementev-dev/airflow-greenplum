# Демобаза bookings в Postgres

Этот каталог используется для работы с демобазой [bookings](https://postgrespro.ru/education/demodb), которая будет источником данных для будущего DWH в Greenplum.

На первом этапе мы:
- поднимаем отдельный контейнер `bookings-db` с Postgres;
- устанавливаем в нём генератор демобазы `demodb` (репозиторий `postgrespro/demodb`);
- генерируем данные «день за днём» с помощью `make`‑команд.

Основные команды см. в корневом `Makefile` (`bookings-init`, `bookings-generate-day`, `bookings-psql`) и в `README.md` проекта.


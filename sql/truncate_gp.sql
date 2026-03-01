-- Скрипт для полной очистки всех слоев DWH (STG, ODS, DDS, DM).
-- Используется для сброса состояния перед проведением E2E-тестов.

-- 1. Слой STG (Сырые данные)
TRUNCATE stg.bookings, 
         stg.tickets, 
         stg.airports, 
         stg.airplanes, 
         stg.routes, 
         stg.seats, 
         stg.flights, 
         stg.segments, 
         stg.boarding_passes;

-- 2. Слой ODS (Текущее состояние, SCD1)
TRUNCATE ods.bookings, 
         ods.tickets, 
         ods.airports, 
         ods.airplanes, 
         ods.routes, 
         ods.seats, 
         ods.flights, 
         ods.segments, 
         ods.boarding_passes;

-- 3. Слой DDS (Схема "Звезда", SCD1/SCD2)
TRUNCATE dds.dim_calendar, 
         dds.dim_airports, 
         dds.dim_airplanes, 
         dds.dim_tariffs, 
         dds.dim_passengers, 
         dds.dim_routes, 
         dds.fact_flight_sales;

-- 4. Слой DM (Витрины данных)
TRUNCATE dm.sales_report;

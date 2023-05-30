-- Carga los datos de la tabla
raw_data = LOAD '$input_path' USING PigStorage(',') AS (
    hotel:chararray,
    is_canceled:int,
    lead_time:int,
    arrival_date_year:int,
    arrival_date_month:chararray,
    arrival_date_week_number:int,
    arrival_date_day_of_month:int,
    stays_in_weekend_nights:int,
    stays_in_week_nights:int,
    adults:int,
    children:int,
    babies:int,
    meal:chararray,
    country:chararray,
    market_segment:chararray,
    distribution_channel:chararray,
    is_repeated_guest:int,
    previous_cancellations:int,
    previous_bookings_not_canceled:int,
    reserved_room_type:chararray,
    assigned_room_type:chararray,
    booking_changes:int,
    deposit_type:chararray,
    agent:int,
    company:int,
    days_in_waiting_list:int,
    customer_type:chararray,
    adr:float,
    required_car_parking_spaces:int,
    total_of_special_requests:int,
    reservation_status:chararray,
    reservation_status_date:chararray
);

-- Filtra las filas con valores nulos en la columna 'hotel'
-- null_hotel = FILTER data BY hotel IS NULL;
-- DESCRIBE null_hotel;
-- Hacer esto con cada columna que quiera analizar

-- Repite el proceso para las demás columnas
-- null_is_canceled = FILTER data BY is_canceled IS NULL;
-- DESCRIBE null_is_canceled;

-- null_lead_time = FILTER data BY lead_time IS NULL;
-- DESCRIBE null_lead_time;

-- Repite este proceso para todas las columnas que desees verificar

-- Filtra los hoteles que fueron cancelados
canceled_hotels = FILTER raw_data BY is_canceled == 1;

-- Calcula la cantidad total de reservas por país
reservations_by_country = FOREACH (GROUP raw_data BY country) GENERATE group AS country, COUNT(raw_data) AS total_reservations;

-- Obtiene la cantidad de reservas por mes y año
reservations_by_month_year = FOREACH (GROUP raw_data BY (arrival_date_year, arrival_date_month)) GENERATE 
    FLATTEN(group) AS (year:int, month:chararray), COUNT(raw_data) AS total_reservations;

-- Definir la ruta de salida deseada
DEFINE output_path_country '/content/resultadoPig/Reservas_por_pais';
DEFINE output_path_year '/content/resultadoPig/Reservas_mes_anyo';
DEFINE output_path_table '/content/resultadoPig/Tabla';

-- Comprobar si la ruta ya existe utilizando comandos del sistema de archivos
fs -test -e $output_path_country;
fs -test -e $output_path_year;
fs -test -e $output_path_table;

-- Si la ruta existe, guardar los resultados allí
-- Si no, crear la ruta y guardar los resultados
STORE reservations_by_country INTO $output_path_country USING PigStorage(',');
STORE reservations_by_month_year INTO $output_path_year USING PigStorage(',');
STORE raw_data INTO $output_path_table USING PigStorage(',');


-- Guarda los resultados en archivos
-- STORE reservations_by_country INTO '/content/resultadoPig/Reservas_por_pais';
-- STORE reservations_by_month_year INTO '/content/resultadoPig/Reservas_mes_anyo';
-- STORE raw_data INTO '/content/resultadoPig/Tabla';

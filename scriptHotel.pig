-- Carga los datos de la tabla
raw_data = LOAD 'HotelBookings.csv' USING PigStorage(',') AS (
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

-- Filtra los hoteles que fueron cancelados
canceled_hotels = FILTER raw_data BY is_canceled == 1;

-- Calcula el tiempo promedio de espera en días para las reservas canceladas
avg_waiting_time = FOREACH canceled_hotels GENERATE AVG((float)days_in_waiting_list) AS avg_wait_time;

-- Calcula la cantidad total de reservas por país
reservations_by_country = FOREACH (GROUP raw_data BY country) GENERATE group AS country, COUNT(raw_data) AS total_reservations;

-- Obtiene la cantidad de reservas por mes y año
reservations_by_month_year = FOREACH (GROUP raw_data BY (arrival_date_year, arrival_date_month)) GENERATE 
    FLATTEN(group) AS (year:int, month:chararray), COUNT(raw_data) AS total_reservations;

-- Guarda los resultados en archivos
STORE avg_waiting_time INTO '/content/resultadoPig/Tiempo_espera_promedio';
STORE reservations_by_country INTO '/content/resultadoPig/Reservas_por_pais';
STORE reservations_by_month_year INTO '/content/resultadoPig/Reservas_mes_anyo';
STORE filtered_data INTO '/content/resultadoPig/AnalisisNull';
DUMP raw_data;

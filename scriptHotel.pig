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
    reservation_status_date_new:chararray
);

-- Filtra y elimina las columnas con valores nulos
filtered_data = FOREACH raw_data GENERATE
    hotel, is_canceled, lead_time, arrival_date_year, arrival_date_month, arrival_date_week_number, arrival_date_day_of_month,
    stays_in_weekend_nights, stays_in_week_nights, adults, children, babies, meal, country, market_segment, distribution_channel,
    is_repeated_guest, previous_cancellations, previous_bookings_not_canceled, reserved_room_type, assigned_room_type,
    booking_changes, deposit_type, agent, company, days_in_waiting_list, customer_type, adr, required_car_parking_spaces,
    total_of_special_requests, reservation_status, reservation_status_date_new
    WHERE hotel is not null and is_canceled is not null and lead_time is not null and arrival_date_year is not null
    and arrival_date_month is not null and arrival_date_week_number is not null and arrival_date_day_of_month is not null
    and stays_in_weekend_nights is not null and stays_in_week_nights is not null and adults is not null and children is not null
    and babies is not null and meal is not null and country is not null and market_segment is not null
    and distribution_channel is not null and is_repeated_guest is not null and previous_cancellations is not null
    and previous_bookings_not_canceled is not null and reserved_room_type is not null and assigned_room_type is not null
    and booking_changes is not null and deposit_type is not null and agent is not null and company is not null
    and days_in_waiting_list is not null and customer_type is not null and adr is not null and required_car_parking_spaces is not null
    and total_of_special_requests is not null and reservation_status is not null and reservation_status_date_new is not null;


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

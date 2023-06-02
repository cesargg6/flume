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

-- Eliminar los valores null
null_company = FILTER raw_data BY company IS NULL;
null_company_seleccionado = FOREACH null_company GENERATE company;

-- Almacenar los resultados en un archivo CSV
-- STORE null_company_seleccionado INTO '/content/resultadoPig/' USING PigStorage(',');

filtered_data = FILTER raw_data BY (
    hotel is not null and
    is_canceled is not null and
    lead_time is not null and
    arrival_date_year is not null and
    arrival_date_month is not null and
    arrival_date_week_number is not null and
    arrival_date_day_of_month is not null and
    stays_in_weekend_nights is not null and
    stays_in_week_nights is not null and
    adults is not null and
    children is not null and
    babies is not null and
    meal is not null and
    country is not null and
    market_segment is not null and
    distribution_channel is not null and
    is_repeated_guest is not null and
    previous_cancellations is not null and
    previous_bookings_not_canceled is not null and
    reserved_room_type is not null and
    assigned_room_type is not null and
    booking_changes is not null and
    deposit_type is not null and
    agent is not null and
    company is not null and
    days_in_waiting_list is not null and
    customer_type is not null and
    adr is not null and
    required_car_parking_spaces is not null and
    total_of_special_requests is not null and
    reservation_status is not null and
    reservation_status_date is not null
);

-- Almacenar los resultados en un archivo CSV
STORE filtered_data INTO '/content/resultadoPig/' USING PigStorage(',');

-- Copiar el archivo a una ubicación final con extensión .csv
-- fs -copyToLocal 'null_company_temp' '$output_path/null_company.csv';

-- DUMP null_company_seleccionado;
DUMP raw_data;

CREATE TABLE vehicle(
id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
device_id VARCHAR(100) NOT NULL,
brand VARCHAR(100) NOT NULL,
operator VARCHAR(100) NOT NULL,
average_mileage BIGINT,
average_daily_mileage BIGINT,
mean_travel_time BIGINT,
average_daily_travel_time BIGINT,
average_daily_trip_times BIGINT,
CONSTRAINT UQ_vehicle UNIQUE (device_id,brand,operator));

CREATE TABLE vehicle_record(
id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
device_id VARCHAR(100) NOT NULL,
brand VARCHAR(100) NOT NULL,
operator VARCHAR(100) NOT NULL,
plate VARCHAR(100) NOT NULL,
vin VARCHAR(100) NOT NULL,
start_mileage BIGINT,
stop_mileage BIGINT,
travel_total_mileage BIGINT,
start_time BIGINT,
stop_time BIGINT,
CONSTRAINT UQ_vehicle_record UNIQUE (device_id,brand,operator,plate,vin));

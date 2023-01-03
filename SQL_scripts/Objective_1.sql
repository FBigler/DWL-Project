-- Checking extreme values --

SELECT id, evse_id, day_occupancy, day_kwh, day_cars, "date"
FROM public.charging_stations_occupancy_day
where day_kwh > 1500;

SELECT id, evse_id, day_occupancy, day_kwh, day_cars, "date"
FROM public.charging_stations_occupancy_day
where day_cars > 10;

-- Objective 1
-- daily average occupancy rate per station over full period

create view daily_average_charging_stations_occupancy
as 
with 
cteDay as (
select evse_id, 
avg(day_occupancy) as avg_occupancy,
avg(day_kwh) as avg_kwh,
avg(day_cars) as avg_cars
from public.charging_stations_occupancy_day
group by evse_id),
cteStatic as (
select evse_id, charging_station_id
from public.charging_stations_static)
select 
round(sum(cteDay.avg_occupancy)) as avg_occupancy,
round(sum(cteDay.avg_kwh)) as avg_kwh,
round(sum(cteDay.avg_cars)) as avg_cars,
round(sum(cteDay.avg_occupancy)/sum(cteDay.avg_cars)) as avg_occupancy_per_car,
round(sum(cteDay.avg_kwh)/sum(cteDay.avg_cars)) as avg_kwh_per_car,
cteStatic.charging_station_id,
coordinate_east as latitude, 
coordinate_nord as longitude
from cteDay 
left join cteStatic 
on cteDay.evse_id = cteStatic.evse_id
left join chargin_stations_location csl 
on cteStatic.charging_station_id = csl.charging_station_id 
group by cteStatic.charging_station_id, longitude, latitude
order by avg_kwh desc;


-- sum occupancy over full period

create view sum_charging_stations_occupancy
as 
with 
cteDay as (
select evse_id, 
sum(day_occupancy) as sum_occupancy,
sum(day_kwh) as sum_kwh,
sum(day_cars) as sum_cars
from public.charging_stations_occupancy_day
group by evse_id),
cteStatic as (
select evse_id, charging_station_id
from public.charging_stations_static)
select 
round(sum(cteDay.sum_occupancy)) as sum_occupancy,
round(sum(cteDay.sum_kwh)) as sum_kwh,
round(sum(cteDay.sum_cars)) as sum_cars,
round(sum(cteDay.sum_occupancy)/sum(cteDay.sum_cars)) as avg_occupancy_per_car,
round(sum(cteDay.sum_kwh)/sum(cteDay.sum_cars)) as avg_kwh_per_car,
cteStatic.charging_station_id,
coordinate_east as latitude, 
coordinate_nord as longitude
from cteDay 
left join cteStatic 
on cteDay.evse_id = cteStatic.evse_id
left join chargin_stations_location csl 
on cteStatic.charging_station_id = csl.charging_station_id 
group by cteStatic.charging_station_id, longitude, latitude
order by sum_kwh desc;


-- Checking averages / sum
-- DB

-- Daily averages
with 
cteDay as (
select evse_id, 
avg(day_occupancy) as avg_occupancy,
avg(day_kwh) as avg_kwh,
avg(day_cars) as avg_cars
from public.charging_stations_occupancy_day
group by evse_id)
select 
round(sum(cteDay.avg_occupancy)) as sum_occupancy,
round(sum(cteDay.avg_kwh)) as sum_kwh,
round(sum(cteDay.avg_cars)) as sum_cars
from cteDay;

-- Monthly sums
with 
cteDay as (
select evse_id, 
sum(day_occupancy) as sum_occupancy,
sum(day_kwh) as sum_kwh,
sum(day_cars) as sum_cars
from public.charging_stations_occupancy_day
group by evse_id)
select 
round(sum(cteDay.sum_occupancy)) as sum_occupancy,
round(sum(cteDay.sum_kwh)) as sum_kwh,
round(sum(cteDay.sum_cars)) as sum_cars
from cteDay;

-- Views
-- view daily_average_charging_stations_occupancy 

select sum(avg_occupancy), sum(avg_kwh), sum(avg_cars)
from "1_daily_average_charging_stations_occupancy" dacso2

-- view sum_charging_stations_occupancy

select sum(sum_occupancy), sum(sum_kwh), sum(sum_cars)
from "1_sum_charging_stations_occupancy" scso2


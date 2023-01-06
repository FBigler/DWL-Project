-- Objective 2 - Understanding the rate of occupancy of the charging stations based on its location

-- per canton

create view canton_sum_charging_stations_occupancy
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
select evse_id, charging_station_id, id_postal_code
from public.charging_stations_static),
cteCantons as (
select id_postal_code, canton_abbreviation
from postal_codes pc)
select 
round(sum(cteDay.sum_occupancy)) as sum_occupancy,
round(sum(cteDay.sum_kwh)) as sum_kwh,
round(sum(cteDay.sum_cars)) as sum_cars,
round(sum(cteDay.sum_occupancy)/sum(cteDay.sum_cars)) as avg_occupancy_per_car,
round(sum(cteDay.sum_kwh)/sum(cteDay.sum_cars)) as avg_kwh_per_car,
cteCantons.canton_abbreviation
from cteDay 
left join cteStatic 
on cteDay.evse_id = cteStatic.evse_id
left join cteCantons
on cteStatic.id_postal_code = cteCantons.id_postal_code
group by cteCantons.canton_abbreviation
order by sum_kwh desc;


-- per region

create view region_sum_charging_stations_occupancy
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
select evse_id, charging_station_id, id_postal_code
from public.charging_stations_static),
cteCantons as (
select id_postal_code, canton_abbreviation
from postal_codes pc),
cteRegions as (
select canton_abbreviation, region_name
from regions_and_cantons rac)
select 
round(sum(cteDay.sum_occupancy)) as sum_occupancy,
round(sum(cteDay.sum_kwh)) as sum_kwh,
round(sum(cteDay.sum_cars)) as sum_cars,
round(sum(cteDay.sum_occupancy)/sum(cteDay.sum_cars)) as avg_occupancy_per_car,
round(sum(cteDay.sum_kwh)/sum(cteDay.sum_cars)) as avg_kwh_per_car,
cteRegions.region_name
from cteDay 
left join cteStatic 
on cteDay.evse_id = cteStatic.evse_id
left join cteCantons
on cteStatic.id_postal_code = cteCantons.id_postal_code
left join cteRegions
on cteCantons.canton_abbreviation = cteRegions.canton_abbreviation
group by cteRegions.region_name
order by sum_kwh desc;



-- Checking sums cantons
-- DB

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
-- view canton_sum_charging_stations_occupancy 

select sum(sum_occupancy), sum(sum_kwh), sum(sum_cars)
from canton_sum_charging_stations_occupancy cscso  

-- view region_sum_charging_stations_occupancy 

select sum(sum_occupancy), sum(sum_kwh), sum(sum_cars)
from region_sum_charging_stations_occupancy rscso 
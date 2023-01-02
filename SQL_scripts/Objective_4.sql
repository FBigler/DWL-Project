-- Objective 4 - Determine the share of renewable energy sources on the entire 
-- charging consumption per canton --

-- Share of renewable energy per region 

create view Regions_renewable_energy_sources_KW_capacity
as 
with 
cteRenewables as (
select canton_abbreviation, round(sum(total_power_kw)) as renewable_power
from electricity_production_plants epp
where id_sub_category in (1,3,6,8) -- w/o 7; Waste 
group by canton_abbreviation),
cteEnergy as (
select canton_abbreviation, round(sum(total_power_kw)) as total_power
from electricity_production_plants epp
group by canton_abbreviation)
select sum(cte1.renewable_power) as renewable_power_region_kw,
sum(cte2.total_power) as total_power_region_kw,
(sum(cte1.renewable_power)/sum(cte2.total_power)) as share_renewables,
regions.region_name 
from cteRenewables as cte1
left join cteEnergy as cte2
on cte1.canton_abbreviation = cte2.canton_abbreviation
left join regions_and_cantons as regions
on cte1.canton_abbreviation = regions.canton_abbreviation 
group by regions.region_name
order by regions.region_name, share_renewables desc;


-- Charging energy consumption / in percent of renewables per region 

-- per region

create view regions_renewable_charging_shares
as 
with 
cte1 as (
SELECT sum_occupancy, sum_kwh, sum_cars, avg_occupancy_per_car, avg_kwh_per_car, canton_abbreviation
FROM public."3_canton_sum_charging_stations_occupancy"),
cteRenewables as (
select canton_abbreviation, round(sum(epp."Avg_monthly_production_kwh")) as renewable_power_kwh
from electricity_production_plants epp
where id_sub_category in (1,3,6,8) -- w/o 7; Waste 
group by canton_abbreviation
),
cteRegions as (
select canton_abbreviation, region_name
from regions_and_cantons rac)
select 
sum(sum_kwh) as sum_kwh_at_charging_stations, 
sum(cteRenewables.renewable_power_kwh) as renewable_power_kwh,
round((sum(sum_kwh/cteRenewables.renewable_power_kwh)*100)::numeric, 4) as percentage_renewable_energy,
cteRegions.region_name
from cte1
left join cteRenewables
on cte1.canton_abbreviation = cteRenewables.canton_abbreviation
left join cteRegions
on cte1.canton_abbreviation = cteRegions.canton_abbreviation
group by cteRegions.region_name
order by percentage_renewable_energy desc;

-- per canton

create view cantons_renewable_charging_shares
as 
with 
cte1 as (
SELECT sum_occupancy, sum_kwh, sum_cars, avg_occupancy_per_car, avg_kwh_per_car, canton_abbreviation
FROM public."3_canton_sum_charging_stations_occupancy"),
cteRenewables as (
select canton_abbreviation, round(sum(epp."Avg_monthly_production_kwh")) as renewable_power_kwh
from electricity_production_plants epp
where id_sub_category in (1,3,6,8) -- w/o 7; Waste 
group by canton_abbreviation
)
select 
sum_kwh as sum_kwh_at_charging_stations, 
cteRenewables.renewable_power_kwh ,
round(((sum_kwh/cteRenewables.renewable_power_kwh)*100)::numeric, 4) as percentage_renewable_energy,
cte1.canton_abbreviation
from cte1
left join cteRenewables
on cte1.canton_abbreviation = cteRenewables.canton_abbreviation
order by percentage_renewable_energy desc;


-- Check shares Renewables KW
-- Renewable total DB
select round(sum(total_power)) as renewable_power
from electricity_production_plants epp
where id_sub_category in (1,3,6,8)
-- All DB
select round(sum(total_power)) as total_power
from electricity_production_plants epp
-- 19236828 / 22947286 = 83% 

-- Totals acc. to view
SELECT sum(renewable_power_region) as sum_renewable, sum(total_power_region) as total_power
FROM public.renewables;

-- official stats say it's correct but consumption looks quite differently than consumption;
-- https://www.uvek-gis.admin.ch/BFE/storymaps/EE_Elektrizitaetsproduktionsanlagen/

-- Check share Renewables from DB
select sum(epp."Avg_monthly_production_kwh") as renewable_power
from electricity_production_plants epp
where id_sub_category in (1,3,6,8)
-- 3'584'498'953 kwh bzw. 3584 gwh 
-- Check shares Charging power kwh from DB
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
-- sum kwh 6'538'821

-- from view "4_cantons_renewable_charging_shares" 
SELECT sum(sum_kwh_at_charging_stations), sum(renewable_power_kwh)
FROM public."4_cantons_renewable_charging_shares";
--- correct!

-- from view "4_regions_renewable_charging_shares" 
SELECT sum(sum_kwh_at_charging_stations), sum(renewable_power_kwh)
FROM public."4_regions_renewable_charging_shares";
--- correct!






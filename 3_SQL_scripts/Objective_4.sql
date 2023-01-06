-- Objective 4 Determine the available charging capacity per registered electric vehicle per canton

-- available charging capacity per registered electric vehicle -- per canton

create view cantons_used_renewable_power
as 
with 
cte1 as (
SELECT sum_kwh_at_charging_stations, renewable_power_kwh, percentage_renewable_energy, canton_abbreviation
FROM public."4_cantons_renewable_charging_shares"),
cteCars as (
SELECT sum(count_2021) as sum_eCars, canton_abbreviation
FROM public.electric_cars_21
group by canton_abbreviation)
select 
cteCars.sum_eCars,
round(((cteCars.sum_eCars * 2000)/cte1.renewable_power_kwh*100)::numeric, 2) as "percentage_2000_kwh_per_car/year",
round(((cteCars.sum_eCars * 2500)/cte1.renewable_power_kwh*100)::numeric, 2) as "percentage_2500_kwh_per_car/year",
cte1.renewable_power_kwh,
cte1.canton_abbreviation
from cte1
left join cteCars
on cte1.canton_abbreviation = cteCars.canton_abbreviation
order by "percentage_2500_kwh_per_car/year" desc;




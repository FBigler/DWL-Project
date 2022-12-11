-- Objective 4 - Determine the share of renewable energy sources on the entire 
-- charging consumption per canton --

-- Charging energy consumption per region --

-- Share of renewable energy per region -- 

create view Renewables
as 
with 
cteRenewables as (
select canton_abbreviation, round(sum(total_power)) as renewable_power
from electricity_production_plants epp
where id_sub_category in (1,3,6,8) -- w/o 7; Waste 
group by canton_abbreviation),
cteEnergy as (
select canton_abbreviation, round(sum(total_power)) as total_power
from electricity_production_plants epp
group by canton_abbreviation)
select sum(cte1.renewable_power) as renewable_power_region,
sum(cte2.total_power) as total_power_region,
(sum(cte1.renewable_power)/sum(cte2.total_power)) as share_renewables,
regions.region_name 
from cteRenewables as cte1
left join cteEnergy as cte2
on cte1.canton_abbreviation = cte2.canton_abbreviation
left join regions_and_cantons as regions
on cte1.canton_abbreviation = regions.canton_abbreviation 
group by regions.region_name
order by regions.region_name, share_renewables desc;

-- Check shares
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

-- official stats say it's correct but consumption looks differently;
-- https://www.uvek-gis.admin.ch/BFE/storymaps/EE_Elektrizitaetsproduktionsanlagen/




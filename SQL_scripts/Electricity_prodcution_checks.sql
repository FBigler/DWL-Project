-- Checking electricity production plants
-- NULL values fossil energy
SELECT xtf_id, id_postal_code, municipality, canton_abbreviation, id_sub_category, total_power_kw, avg_monthly_production_kwh
FROM public.electricity_production_plants where id_sub_category in (2,4,5,7) and avg_monthly_production_kwh != NULL;
-- No NULL values renewables
SELECT xtf_id, id_postal_code, municipality, canton_abbreviation, id_sub_category, total_power_kw, avg_monthly_production_kwh
FROM public.electricity_production_plants where id_sub_category in (1,3,6,8) and avg_monthly_production_kwh != NULL;
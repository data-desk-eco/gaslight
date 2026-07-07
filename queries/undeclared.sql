-- undeclared flaring: vnf flare sites burning over leases that report
-- producing gas but declare zero flared/vented (pdq disposition 04).
-- run: duckdb -readonly data/data.duckdb < queries/undeclared.sql
-- findings memo: docs/undeclared-flaring.md

-- leases within 375m (viirs half-pixel) of each vnf site
create temp table site_lease as
select v.flare_id, w.oil_gas_code, dm.pdq_district, w.lease_district, w.lease_number,
       any_value(w.lease_name) lease_name, any_value(w.operator_name) operator_name
from permian.vnf_sites v
join permian.wells_tx w
  on w.latitude between v.lat - 0.0034 and v.lat + 0.0034
 and w.longitude between v.lon - 0.0034 and v.lon + 0.0034
join rrc.district_map dm on dm.rrc_district = w.lease_district
where sqrt(pow((w.latitude - v.lat) * 111320, 2)
         + pow((w.longitude - v.lon) * 111320 * cos(radians(v.lat)), 2)) <= 375
group by all;

-- the rrc well file keys gas wells by stale ids, so pdq 'G' records (per-well
-- gas ids) never match the well join: ~53 bcf of declared gas-well flaring
-- since 2021 was invisible (the hamburglar case exposed this). repair: link
-- in-pixel oil leases to gas ids sharing (district, operator, lease name) in
-- pdq, and fold those into each site's lease set
create temp table pdq_o as
select district_no, lease_no, any_value(lease_name) lease_name, any_value(operator_no) operator_no
from raw.pdq_leases where oil_gas_code = 'O' group by 1, 2;
create temp table pdq_g as
select district_no, lease_no, any_value(lease_name) lease_name, any_value(operator_no) operator_no,
       any_value(operator_name) operator_name
from raw.pdq_leases where oil_gas_code = 'G' group by 1, 2;

insert into site_lease
select distinct sl.flare_id, 'G', sl.pdq_district, sl.lease_district,
       lpad(g.lease_no, 6, '0'), g.lease_name, g.operator_name
from site_lease sl
join pdq_o o on o.district_no = sl.pdq_district and lpad(o.lease_no, 6, '0') = sl.lease_number
join pdq_g g on g.district_no = o.district_no and g.operator_no = o.operator_no and g.lease_name = o.lease_name;

-- sites within 1km of a gas plant flare outside lease reporting; exclude.
-- the r-3 list alone misses most major permian plants (panther, jameson,
-- sterling, dollarhide...), so union in the eia-757 survey locations
create temp table plant_sites as
select distinct v.flare_id
from permian.vnf_sites v
join (select latitude, longitude from permian.facilities
      union all
      select latitude, longitude from read_csv('data/eia_plants.csv')) f
  on f.latitude between v.lat - 0.01 and v.lat + 0.01
 and f.longitude between v.lon - 0.012 and v.lon + 0.012
where sqrt(pow((f.latitude - v.lat) * 111320, 2)
         + pow((f.longitude - v.lon) * 111320 * cos(radians(v.lat)), 2)) <= 1000;

-- plant-scale complexes confirmed by satellite imagery review but absent from
-- both facility lists: 9120 (32.178 -102.267, plant ~300m s of site, midmar/
-- fasken system), 7657 (31.345 -101.798, benedum complex sprawl), 7831
-- (31.833 -101.685) and 8396 (31.846 -101.772), both glasscock co plants
insert into plant_sites values (9120), (7657), (7831), (8396);

-- per site-month: gas produced and disposition-04 declared, summed over all
-- leases in the pixel. universe is raw.lease_production (all producing
-- lease-months); raw.gas_disposition only holds months with flaring declared
create temp table rep as
select sl.flare_id, make_date(lp.cycle_year::int, lp.cycle_month::int, 1) m,
       sum(coalesce(lp.lease_gas_prod_vol, 0) + coalesce(lp.lease_csgd_prod_vol, 0)) prod_mcf,
       sum(coalesce(gd.lease_gas_dispcd04_vol, 0) + coalesce(gd.lease_csgd_dispcde04_vol, 0)) flared_mcf,
       count(*) reporting_leases
from site_lease sl
join raw.lease_production lp
  on lp.oil_gas_code = sl.oil_gas_code and lp.district_no = sl.pdq_district
 and lpad(lp.lease_no, 6, '0') = sl.lease_number
left join raw.gas_disposition gd
  on gd.oil_gas_code = lp.oil_gas_code and gd.district_no = lp.district_no
 and gd.lease_no = lp.lease_no and gd.cycle_year = lp.cycle_year and gd.cycle_month = lp.cycle_month
group by 1, 2;

create temp table cmp as
select d.flare_id, d.m, d.det_days, d.mw,
       coalesce(r.flared_mcf, 0) flared_mcf, coalesce(r.prod_mcf, 0) prod_mcf,
       coalesce(r.reporting_leases, 0) reporting_leases
from (select flare_id, date_trunc('month', date) m, count(*) det_days, sum(rh_mw) mw
      from permian.vnf_detections group by 1, 2) d
left join rep r on r.flare_id = d.flare_id and r.m = d.m
where d.flare_id in (select flare_id from site_lease)
  and d.flare_id not in (from plant_sites)
  and d.m between date '2021-01-01' and date '2025-10-01';

-- headline: sustained (>=3 nights) site-months by declaration status
select case when reporting_leases = 0 then 'no pdq filing'
            when flared_mcf = 0 and prod_mcf > 0 then 'producing, zero flaring declared'
            when flared_mcf = 0 then 'filed, no production, zero declared'
            else 'flaring declared' end bucket,
       count(*) site_months, count(distinct flare_id) sites,
       sum(det_days) nights, round(sum(mw)) mw
from cmp where det_days >= 3 group by 1 order by 5 desc;

-- worst sites: undeclared months, robust to pixel wobble (nothing declared
-- within 750m either)
create temp table sl750 as
select v.flare_id, w.oil_gas_code, dm.pdq_district, w.lease_number
from permian.vnf_sites v
join permian.wells_tx w
  on w.latitude between v.lat - 0.0068 and v.lat + 0.0068
 and w.longitude between v.lon - 0.0068 and v.lon + 0.0068
join rrc.district_map dm on dm.rrc_district = w.lease_district
where sqrt(pow((w.latitude - v.lat) * 111320, 2)
         + pow((w.longitude - v.lon) * 111320 * cos(radians(v.lat)), 2)) <= 750
group by all;

insert into sl750
select distinct sl.flare_id, 'G', sl.pdq_district, lpad(g.lease_no, 6, '0')
from sl750 sl
join pdq_o o on o.district_no = sl.pdq_district and lpad(o.lease_no, 6, '0') = sl.lease_number
join pdq_g g on g.district_no = o.district_no and g.operator_no = o.operator_no and g.lease_name = o.lease_name;

create temp table rep_wide as
select sl.flare_id, make_date(gd.cycle_year::int, gd.cycle_month::int, 1) m,
       sum(coalesce(gd.lease_gas_dispcd04_vol, 0) + coalesce(gd.lease_csgd_dispcde04_vol, 0)) flared_mcf
from sl750 sl
join raw.gas_disposition gd
  on gd.oil_gas_code = sl.oil_gas_code and gd.district_no = sl.pdq_district
 and lpad(gd.lease_no, 6, '0') = sl.lease_number
group by 1, 2;

select c.flare_id, any_value(v.lat) lat, any_value(v.lon) lon,
       count(*) filter (c.flared_mcf = 0 and c.prod_mcf > 0 and c.det_days >= 3
                        and coalesce(rw.flared_mcf, 0) = 0) undecl_months,
       sum(c.det_days) filter (c.flared_mcf = 0 and c.prod_mcf > 0
                               and coalesce(rw.flared_mcf, 0) = 0) undecl_nights,
       round(sum(c.mw) filter (c.flared_mcf = 0 and c.prod_mcf > 0
                               and coalesce(rw.flared_mcf, 0) = 0)) undecl_mw,
       count(*) filter (c.flared_mcf > 0) decl_months,
       (select string_agg(distinct operator_name, ' | ') from site_lease sl
        where sl.flare_id = c.flare_id) operators
from cmp c
join permian.vnf_sites v on v.flare_id = c.flare_id
left join rep_wide rw on rw.flare_id = c.flare_id and rw.m = c.m
group by c.flare_id having undecl_months >= 6
order by undecl_mw desc limit 25;

-- operator rollup (single-operator sites only, unambiguous attribution)
select so.op operator_name, count(distinct c.flare_id) sites,
       count(*) filter (c.flared_mcf = 0 and c.prod_mcf > 0 and c.det_days >= 3) undecl_months,
       sum(c.det_days) filter (c.flared_mcf = 0 and c.prod_mcf > 0) undecl_nights,
       round(sum(c.mw) filter (c.flared_mcf = 0 and c.prod_mcf > 0)) undecl_mw,
       count(*) filter (c.flared_mcf > 0) decl_months
from cmp c
join (select flare_id, any_value(operator_name) op from site_lease
      group by 1 having count(distinct operator_name) = 1) so using (flare_id)
group by 1 having undecl_months >= 12 order by undecl_mw desc limit 20;

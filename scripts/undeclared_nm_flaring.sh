#!/usr/bin/env bash
# undeclared flaring in the new mexico permian.
# finds s2 flare-detection sites that vnf independently confirms (viirs, lower res)
# yet carry no matching nm ocd flare/vent notification -- i.e. combustion two
# satellites agree on, with nothing filed to the state.
#
# usage: scripts/undeclared_nm_flaring.sh
set -euo pipefail
DB="${1:-dist/gaslight.duckdb}"
cd "$(dirname "$0")/.."

cat <<'TXT'
================================================================================
 UNDECLARED FLARING — NEW MEXICO PERMIAN
 s2 (10 m) + vnf (viirs 750 m) agree on combustion, no nm ocd notice on file
================================================================================

method
  universe   s2 flare sites clipped to the nm box (lat >= 32, lon <= -103.061 --
             the same footprint the nm ocd notifications occupy). s2 window is
             2025-01..2026-05; nm flare/vent notices run 2021..2026.
  confirmed  a vnf nightfire site sits within 375 m (one viirs m-band pixel) of
             the s2 site -- two independent sensors, different physics, agree.
  undeclared no nm ocd notice of type Flare / Vent / Vent with Flaring lies
             within the match radius at ANY time on record (2021+). the "ever"
             test is deliberately generous to the operator: a site only survives
             if nothing was filed nearby across the whole 5.5-year window.
  radius     375 m primary (viirs pixel); sensitivity at 1 km / 2 km reported.
  caveat     nm notices are self-reported and lease/well-geocoded, so a real
             notice can land a few hundred metres off. treat survivors as leads
             to verify on imagery, not proven violations. self-reported volumes
             are not independent observations.
TXT

duckdb -box "$DB" <<'SQL'
-- planar metres, fine at 32 N
create or replace macro m(dlat,dlon) as
  sqrt((dlat*111000.0)*(dlat*111000.0)+(dlon*94300.0)*(dlon*94300.0));

create or replace temp table s2nm as
  select * from s2_detections where lat>=32 and lon<=-103.061;

create or replace temp table notif as
  select latitude lat, longitude lon, incident_type, incident_date, operator,
         facility_name, well_name, volume_released
  from nm_notifications
  where incident_type in ('Flare','Vent','Vent with Flaring');

-- s2 sites with a confirming vnf pixel within 375 m
create or replace temp table confirmed as
  select s.*,
    (select min(m(v.lat-s.lat,v.lon-s.lon)) from vnf_sites v
      where m(v.lat-s.lat,v.lon-s.lon)<=375) vnf_dist
  from s2nm s
  where exists(select 1 from vnf_sites v where m(v.lat-s.lat,v.lon-s.lon)<=375);

.print '── funnel ───────────────────────────────────────────────────────────────'
select
  (select count(*) from s2_detections)                    "s2 sites (permian)",
  (select count(*) from s2nm)                             "  in nm box",
  (select count(*) from confirmed)                        "  + vnf-confirmed",
  (select count(*) from confirmed c where not exists(
     select 1 from notif n where m(n.lat-c.lat,n.lon-c.lon)<=375))
                                                          "  undeclared @375m";

.print ''
.print '── sensitivity: undeclared count vs match radius ────────────────────────'
select r "radius",
  (select count(*) from confirmed c
     where not exists(select 1 from notif n where m(n.lat-c.lat,n.lon-c.lon)<=r))
   "undeclared",
  (select count(*) from confirmed) - (select count(*) from confirmed c
     where not exists(select 1 from notif n where m(n.lat-c.lat,n.lon-c.lon)<=r))
   "declared"
from (values (375.0),(1000.0),(2000.0)) t(r);
SQL

cat <<'TXT'

── the cases (375 m match radius, ranked by s2 total_score) ────────────────────
TXT

duckdb -box "$DB" <<'SQL'
create or replace macro m(dlat,dlon) as
  sqrt((dlat*111000.0)*(dlat*111000.0)+(dlon*94300.0)*(dlon*94300.0));
create or replace temp table s2nm as
  select * from s2_detections where lat>=32 and lon<=-103.061;
create or replace temp table notif as
  select latitude lat, longitude lon, incident_type from nm_notifications
  where incident_type in ('Flare','Vent','Vent with Flaring');
create or replace temp table confirmed as
  select s.*, (select min(m(v.lat-s.lat,v.lon-s.lon)) from vnf_sites v
                 where m(v.lat-s.lat,v.lon-s.lon)<=375) vnf_dist
  from s2nm s
  where exists(select 1 from vnf_sites v where m(v.lat-s.lat,v.lon-s.lon)<=375);
create or replace temp table undeclared as
  select * from confirmed c
  where not exists(select 1 from notif n where m(n.lat-c.lat,n.lon-c.lon)<=375);

-- enrich: nearest vnf site (heat/flow), nearest well (operator lead),
-- distance to closest flare/vent notice of any age (the "gap")
select
  u.h3,
  round(u.lat,5) lat, round(u.lon,5) lon,
  u.first_date, u.last_date, u.n_dates dates,
  round(u.max_b12,3) max_b12, round(u.total_score,1) score, u.corroborated corr,
  round(u.vnf_dist) "vnf_m",
  v.detection_days vnf_days, round(v.avg_rh_mw,1) vnf_mw, round(v.avg_flow_rate,2) vnf_bcm,
  round((select min(m(n.latitude-u.lat,n.longitude-u.lon)) from nm_notifications n
     where n.incident_type in ('Flare','Vent','Vent with Flaring')
       and abs(n.latitude-u.lat)<0.05 and abs(n.longitude-u.lon)<0.05)) "nearest_notice_m",
  w.operator nearest_operator, w.well_name nearest_well, round(w.wd) "well_m"
from undeclared u
left join lateral (
  select detection_days, avg_rh_mw, avg_flow_rate
  from vnf_sites v where m(v.lat-u.lat,v.lon-u.lon)<=375
  order by m(v.lat-u.lat,v.lon-u.lon) limit 1) v on true
left join lateral (
  select operator, well_name, m(w.latitude-u.lat,w.longitude-u.lon) wd
  from wells_nm w
  where abs(w.latitude-u.lat)<0.02 and abs(w.longitude-u.lon)<0.02
  order by wd limit 1) w on true
order by u.total_score desc;
SQL

cat <<'TXT'

── who the leads point to (nearest nm well operator per undeclared site) ────────
TXT

duckdb -box "$DB" <<'SQL'
create or replace macro m(dlat,dlon) as
  sqrt((dlat*111000.0)*(dlat*111000.0)+(dlon*94300.0)*(dlon*94300.0));
create or replace temp table s2nm as
  select * from s2_detections where lat>=32 and lon<=-103.061;
create or replace temp table notif as
  select latitude lat, longitude lon, incident_type from nm_notifications
  where incident_type in ('Flare','Vent','Vent with Flaring');
create or replace temp table undeclared as
  select s.* from s2nm s
  where exists(select 1 from vnf_sites v where m(v.lat-s.lat,v.lon-s.lon)<=375)
    and not exists(select 1 from notif n where m(n.lat-s.lat,n.lon-s.lon)<=375);
select coalesce(w.op,'(no well within 2 km)') "operator", count(*) sites,
       round(avg(u.total_score),1) avg_score, round(max(u.max_b12),3) max_b12
from undeclared u
left join lateral (
  select operator op from wells_nm w
  where abs(w.latitude-u.lat)<0.02 and abs(w.longitude-u.lon)<0.02
  order by m(w.latitude-u.lat,w.longitude-u.lon) limit 1) w on true
group by 1 order by sites desc, avg_score desc;
SQL

cat <<'TXT'

────────────────────────────────────────────────────────────────────────────────
 read: each site above is combustion that s2 (10 m) and vnf (viirs) both detected
 in the nm permian, with no state flare/vent notice within 375 m across 2021-2026.
 vnf_mw = mean radiant heat; vnf_bcm = mean modelled flow. widen the radius and
 the list shrinks (see sensitivity) -- the 1 km / 2 km survivors are the hardest
 to explain as mere geocoding slop. verify each on recent imagery before use.
────────────────────────────────────────────────────────────────────────────────
TXT

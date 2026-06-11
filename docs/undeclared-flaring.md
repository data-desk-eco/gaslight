# undeclared flaring in the permian basin

are operators flaring without declaring it? satellite flare observations (VIIRS
nightfire + sentinel-2) cross-matched against the flaring volumes operators
declare to the RRC (PDQ gas disposition code 04 — gas vented/flared) say yes:
roughly a quarter of all sustained satellite-observed flare activity at
producing leases has **zero** flared gas declared for the month in question.

reproduce with `duckdb -readonly data/data.duckdb < queries/undeclared.sql`.

## method

- for each of the 1,297 VNF flare sites, collect all leases with a well inside
  375m (the VIIRS half-pixel match radius used throughout gaslight), via
  `permian.wells` → (`oil_gas_code`, district, lease number).
- exclude sites within 1km of an R-3 gas processing facility (63 sites) —
  plant flaring is not reported through lease-level PDQ dispositions.
- for each site-month with VNF detections, sum *declared* flared volumes
  (`raw.gas_disposition`, disposition 04, gas + casinghead) and *produced*
  volumes (`raw.lease_production`) across every matched lease. note
  `raw.gas_disposition` only contains lease-months where flaring was declared;
  the production file is the full universe, which is what makes a true zero
  observable.
- window 2021-01 .. 2025-10 (PDQ filings lag; later months excluded to avoid
  counting filings not yet made). "sustained" = flare seen on ≥3 nights in the
  month.
- robustness: for headline cases, the test is repeated with every lease within
  750m (a full VIIRS pixel, to rule out pixel wobble blaming a neighbour) —
  the top cases all survive.

## headline numbers (sustained site-months, 2021–2025)

| status of leases in the pixel | site-months | sites | flare-nights | radiant heat (MW) |
|---|---|---|---|---|
| flaring declared (>0 MCF) | 8,228 | 651 | 66,766 | 67,217 |
| **producing gas, zero flaring declared** | **3,410** | **435** | **28,270** | **27,594** |
| filed PDQ, no production, zero declared | 2,111 | 178 | 19,161 | 21,811 |
| no PDQ filing at all | 1,669 | 116 | 15,801 | 28,362 |

the "producing, zero declared" row is the core finding: the satellite watched
a flare burn for ≥3 nights (median far more) in 3,410 site-months where every
lease within the pixel reported gas production but not one MCF flared. counting
all detection months (not just sustained ones) the undeclared bucket spans 599
sites, 32,785 flare-nights and ~30,800 MW of radiant heat.

scale: among site-months where flaring *was* declared, the median declared
volume per MW of observed radiant heat is ~600 MCF (IQR 78–2,157). applied to
the undeclared bucket that implies very roughly **~18 BCF of flared gas over
five years that never appears in RRC disposition records** — treat as an order
of magnitude, not an estimate (the MCF/MW relationship is noisy and skewed).

the "filed, no production" bucket is largely completion flowback — flares
burning before first production is reported — a separate disclosure gap with a
different excuse. the "no PDQ filing" bucket includes inactive/transferred
leases and midstream equipment (compressor stations are not in the R-3 list).

## cases worth a closer look

**citation oil & gas — jameson reef unit (7C-000621), coke county**
(site 8002, 32.0487 −100.6847). the single biggest undeclared signal in the
basin: 51 months with sustained fire and zero declared, 909 flare-nights,
3,743 MW. burned 179–247 nights every year 2021–2025; citation declared
nothing at all until 2024, then 7,181 MCF — against production that fell from
378 MMCF (2021) to 14 MMCF (2025). a flare that burns two nights in three
while the lease reports zero flaring for three straight years is hard to
read as anything but non-reporting. (no S2 corroboration — site may fall
below the score-capped S2 catalogue, whose absence is not evidence of
absence.)

**scout energy management / union oil of california — near the TX-NM border,
winkler county** (site 7948, 32.1498 −103.0563). the most persistent: 57
undeclared months — *every* sustained month, 2021 through 2025; **nothing has
ever been declared at this site**. 1,146 flare-nights while the matched leases
produced 1.0–1.6 BCF/yr. S2 sees a fire 27m away burning in **97.6%** of clear
observations in 2025–26. five years of continuous flaring, >6 BCF/yr-scale
gas throughput, zero MCF declared.

**discovery operating — apache flats, upton county** (site 7372, 31.6417
−102.1919). zero flaring ever declared, yet: VNF saw fire on 1,480 days since
2021 (225–316 nights/yr); an S2 cluster sits 175–260m away with persistence up
to **0.90**; carbon mapper methane plumes were imaged 205–260m away in 2021
and 2025; and discovery itself filed five SWR 32 flaring permits at apache
flats (457m away, release rates up to 8,150 MCF/day) — but those permits
cover ~40 days total, against five years of observed burning. reported lease
production is tiny (0.5–13 MMCF/yr), so either the flare burns gas that never
makes the production report, or the reported production itself is suspect.

**fasken oil and ranch — fee "BM" (08-042190), midland county** (site 9120,
32.1781 −102.2669). episodic but ferocious: avg 20.4 MW per detection (vs
~1–6 for the others). in 2023 the satellite logged 25 flare-nights and 626 MW
while the lease produced 2.15 BCF and declared zero flared. declarations only
begin in 2024 (14,230 MCF) — after three years of observed undeclared burns.

**pioneer natural resources — benedum /spraberry/ unit (7C-005204), upton
county** (site 7657, 31.3447 −101.7979). ~300 nights/yr of fire 2021–2023 with
zero declared; token declarations (635/895 MCF/yr) begin only in 2024. S2
corroborates at 0.82 persistence 195m away. pioneer (now exxon) is also the
basin's #2 in aggregate undeclared signal across 38 single-operator sites.

also notable: **trigeo energy** (site 7690, garden city — 41 undeclared
months, never declared, S2 persistence 0.87 at 27m); **anadarko/oxy** (42
single-operator sites, 211 undeclared months — high frequency, low intensity);
**xto** site 8625 (pyke/merrick units, howard county) with 31 undeclared
months *and* repeated carbon mapper plumes up to ~2,000 kg/hr nearby.

## caveats

- proximity is not ownership (the project's own rule). 375m is a satellite
  pixel; for the named cases the 750m robustness check and single-operator
  pixels make misattribution unlikely, but a well list is not a deed.
- PDQ reports can be amended; "zero declared" means zero in the current RRC
  extract.
- VNF radiant heat → volume conversion is indicative only.
- the S2 catalogue is score-capped (`S2_LIMIT`) and covers 2025-01 onward
  only; missing S2 corroboration means nothing.
- some undeclared fire is plausibly legal-but-misfiled (e.g. volumes reported
  on a different lease id than the wells suggest) — that is still a data
  integrity finding, just a different headline.

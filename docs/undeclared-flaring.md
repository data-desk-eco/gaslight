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
- exclude sites within 1km of a gas processing plant — plant flaring is not
  reported through lease-level PDQ dispositions. the exclusion list unions the
  RRC R-3 facilities with the EIA-757 plant survey (`data/eia_plants.csv`,
  `make eia`): the R-3 list alone misses most major permian plants (panther,
  jameson, sterling, dollarhide...), which silently contaminated an earlier
  version of this analysis with plant flares. four further plant-scale
  complexes found by imagery review but absent from both lists (sites 9120,
  7657, 7831, 8396) are excluded by hand in the query.
- every headline case below was visually confirmed against satellite imagery
  to sit on lease infrastructure (well pads, tank batteries), not midstream
  plant — eyeball the imagery before believing a match.
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
| flaring declared (>0 MCF) | 8,009 | 635 | 64,508 | 57,002 |
| **producing gas, zero flaring declared** | **2,962** | **414** | **21,940** | **14,507** |
| filed PDQ, no production, zero declared | 1,884 | 168 | 15,646 | 10,942 |
| no PDQ filing at all | 1,585 | 112 | 14,765 | 25,543 |

the "producing, zero declared" row is the core finding: the satellite watched
a flare burn for ≥3 nights (median far more) in 2,962 site-months where every
lease within the pixel reported gas production but not one MCF flared. counting
all detection months (not just sustained ones) the undeclared bucket spans 576
sites, 26,219 flare-nights and ~17,000 MW of radiant heat.

scale: among site-months where flaring *was* declared, the median declared
volume per MW of observed radiant heat is ~730 MCF (IQR 164–2,300). applied to
the undeclared bucket that implies very roughly **~10.6 BCF of flared gas over
five years that never appears in RRC disposition records** — treat as an order
of magnitude, not an estimate (the MCF/MW relationship is noisy and skewed).

the "filed, no production" bucket is largely completion flowback — flares
burning before first production is reported — a separate disclosure gap with a
different excuse. the "no PDQ filing" bucket includes inactive/transferred
leases and midstream equipment (compressor stations are not in either plant
list).

## cases worth a closer look

all three confirmed against imagery as lease infrastructure, all survive the
750m wobble check. (two further candidates from an earlier draft — pioneer's
arnett and oxyrock's jones "a", both glasscock county — fell to the imagery
check as plants and are now in the manual exclusion list.)

**anadarko/oxy — loving county** (site 7201, 31.8957 −103.7001). **nothing has
ever been declared at this site**: 44 undeclared months, 466 flare-nights,
2021 through 2025, while the matched lease produced 277–1,041 MMCF/yr. the
fire escalated sharply in 2024 (174 nights, 347 MW). plumes were imaged nearby
in 2021 (158 kg/hr) and 2025 (439 kg/hr). anadarko also tops the frequency
rollup — 211 undeclared months across 42 single-operator sites.

**xto — pyke & merrick units, martin county** (site 8625, 32.3727 −101.8379).
the gap case: xto declared 2,063–4,330 MCF/yr in 2021–23, then in 2024 — the
flare's biggest year on record, 107 nights and 561 MW — the leases declared
zero. declarations resume in 2025 (31,181 MCF). carbon mapper plumes up to
~3,400 kg/hr have been imaged around the pyke/merrick pads. burning more than
ever in the one year the paperwork goes quiet is hard to read as an oversight.

**highpeak energy — o'daniel ranch / jasmine units, howard county** (site
8567, 32.4542 −101.3008). the counter-example that sharpens the rest: highpeak
*does* declare — up to 260,734 MCF in 2023, among the basin's biggest — yet
still logs 25 sustained months with zero declared while the flare burned up to
276 nights/yr. across its 3 sites: 70 undeclared months against only 19
declared. reporting that switches on and off while the fire doesn't.

xto also now tops the single-operator rollup by radiant heat (10 sites, 84
undeclared months, 1,264 MW). also notable: **diamondback** (44 sites, 171
undeclared months), **blackbeard operating** (8 sites, 105 undeclared months),
**trp operating** (6 sites, 82 undeclared months), **pioneer** (33 sites, 56
undeclared months).

## caveats

- proximity is not ownership (the project's own rule). 375m is a satellite
  pixel; for the named cases the 750m robustness check and single-operator
  pixels make misattribution unlikely, but a well list is not a deed.
- the plant exclusion is only as good as the plant lists. R-3 is badly
  incomplete; EIA-757 locations date to 2017, so plants built since are
  invisible. compressor stations appear in neither. headline cases were
  screened by imagery review, but the aggregate buckets surely retain some
  midstream contamination — treat site-level claims as requiring the imagery
  check, always.
- PDQ reports can be amended; "zero declared" means zero in the current RRC
  extract.
- VNF radiant heat → volume conversion is indicative only.
- the S2 catalogue is score-capped (`S2_LIMIT`) and covers 2025-01 onward
  only; missing S2 corroboration means nothing.
- some undeclared fire is plausibly legal-but-misfiled (e.g. volumes reported
  on a different lease id than the wells suggest) — that is still a data
  integrity finding, just a different headline.

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
- **gas-well repair**: the RRC well file keys gas wells by stale ids, so PDQ
  'G' records (per-well gas ids) never match a spatial well join — ~53 BCF of
  declared gas-well flaring since 2021 was invisible to an earlier version of
  this analysis (the hamburglar case exposed it). each site's lease set is
  therefore expanded with the gas ids sharing (district, operator, lease name)
  with an in-pixel oil lease, for both production and disposition. this moved
  ~870 sustained site-months from the zero buckets into "declared" — e.g.
  most of blackbeard operating's apparent gap was gas-id declarations.
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
| flaring declared (>0 MCF) | 8,881 | 682 | 71,698 | 60,984 |
| **producing gas, zero flaring declared** | **3,117** | **438** | **23,315** | **15,515** |
| filed PDQ, no production, zero declared | 1,453 | 144 | 11,848 | 8,698 |
| no PDQ filing at all | 989 | 71 | 9,998 | 22,797 |

the "producing, zero declared" row is the core finding: the satellite watched
a flare burn for ≥3 nights (median far more) in 3,117 site-months where every
lease within the pixel reported gas production but not one MCF flared. counting
all detection months (not just sustained ones) the undeclared bucket spans 614
sites, 27,531 flare-nights and ~17,800 MW of radiant heat. (the gas-well repair
*grew* this bucket on net: it shrank the two no-reporting rows far more, by
revealing production at sites previously presumed inactive.)

scale: among site-months where flaring *was* declared, the median declared
volume per MW of observed radiant heat is ~705 MCF (IQR 163–2,211). applied to
the undeclared bucket that implies very roughly **~10.8 BCF of flared gas over
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
ever been declared at this site** — not on the tabasco cat 54-43-6 oil lease,
not on its gas-well id either: 43 undeclared months, 458 flare-nights, 2021
through 2025, while the matched leases produced 0.45–1.37 BCF/yr combined.
sentinel-2 places the flame 178m from anadarko's wellheads; anadarko declares
routinely elsewhere (~850 MMCF/yr across 1,500+ leases), just never here.
plumes were imaged nearby in 2021 (158 kg/hr) and 2025 (439 kg/hr). anadarko
also tops the frequency rollup — 199 undeclared months across 40
single-operator sites.

the 2024 spike at this site (174 nights, 347 MW) is a second story: it
coincides exactly — and twins with neighbouring site 7289 — with admiral
permian's SWR 32 flaring exceptions at the adjacent hamburglar pads (filings
24522/24890/25209, jun 18–oct 14 2024, 3,500–7,500 MCF/d permitted). admiral's
hamburglar gas wells produced 3.1 BCF in 2024 and declared zero; their only
declaration ever is 31.3 MMCF in october 2025, under per-well gas ids, exactly
matching the next SWR 32 window (filing 30989, 14,500 MCF/d, oct 14–28) and a
439 kg/hr carbon mapper plume on oct 3. permitted-to-flare but
declared-nothing is its own disclosure gap — the SWR 32 paper trail proves
the operator knew it was flaring.

**xto — pyke & merrick units, martin county** (site 8625, 32.3727 −101.8379).
the gap case: xto declared 2,063–4,330 MCF/yr in 2021–23, then in 2024 — the
flare's biggest year on record, 107 nights and 561 MW — all five pyke/merrick
leases declared zero, in lockstep, while still producing 1.3 BCF. declarations
resume in 2025 (30,511 MCF through october). this is not a company-wide lapse:
xto declared 708 MMCF across 478 *other* leases in 2024. and it is xto's own
flare — the company filed "merrick CTB flare" SWR 32 exceptions 45m from the
satellite fix, on the tank battery visible in imagery. carbon mapper plumes up
to ~3,400 kg/hr have been imaged around the pyke/merrick pads. burning more
than ever in the one year the paperwork goes quiet is hard to read as an
oversight.

**highpeak energy — o'daniel ranch / jasmine units, howard county** (site
8567, 32.4542 −101.3008). the counter-example that sharpens the rest: highpeak
*does* declare — up to 260,734 MCF in 2023, among the basin's biggest — yet
still logs 25 sustained months with zero declared while the flare burned up to
276 nights/yr. across its 3 sites: 70 undeclared months against only 19
declared. and after march 2025 highpeak's disposition-04 filings stop
*basin-wide* (34 MMCF total for 2025, vs ~2,500 in each of 2023 and 2024),
while the case leases produced another ~0.85 BCF through october and the flare
burned 50 more nights. reporting that switches on and off while the fire
doesn't.

xto also now tops the single-operator rollup by radiant heat (10 sites, 84
undeclared months, 1,264 MW). also notable: **diamondback** (44 sites, 171
undeclared months), **trp operating** (4 sites, 78 undeclared months),
**upcurve energy** (4 sites, 31 undeclared months, 505 MW), **pioneer** (33
sites, 56 undeclared months). blackbeard operating, prominent in an earlier
draft (105 undeclared months), is largely exonerated by the gas-well repair —
its flaring was declared under gas ids (12 undeclared months remain).

## case validation

each headline case was tested against the alternatives that could explain a
satellite fire with zero declared flaring:

- **midstream facility (compressor station / gas plant)**: no facility within
  5km of any case site in the R-3 list, the EIA-757 survey, or EPA GHGRP
  reporters (nearest: horseshoe draw CGF 5.4km from 7201; panther city CS
  5.7km from 8625; east vealmoor plant 6.6km from 8567). TCEQ STEERS
  emissions-event search returns nothing for highpeak/howard, xto/martin or
  admiral/loving; the loving county compressor stations that do report events
  (barbarian, north silvertip) are elsewhere. decisive: at every case site the
  *named operator itself* filed SWR 32 flare exceptions at the satellite fix
  (xto "merrick CTB flare" 45m; highpeak "jasmine CTB"/"ODR CTB" 100–312m;
  admiral "hamburglar flare" at its pad) — the RRC's own records identify
  these as lease flares.
- **imagery**: esri world imagery shows tank-battery pads under the satellite
  fix at 8625 and 8567; at 7201 the 2025 sentinel-2 flame pixel
  (31.8966 −103.6994) sits 178m from anadarko's tabasco cat wellheads.
- **lease-coverage failure (declared under an id the well join can't see)**:
  swept *by name* across all of PDQ — every lease id ever associated with the
  case lease names, oil or gas. xto and highpeak survive (no hidden
  declarations; the dormant merrick allocation lease last declared 2019).
  admiral's hamburglar did fail this test — its oct 2025 declaration sits
  under gas ids — which is what prompted the gas-well repair now baked into
  the pipeline; its 2024 flowback remains undeclared under every id.
- **company-wide reporting outage**: xto declared on 478 other leases in 2024;
  anadarko on ~1,500 leases every year. the zeros are site-specific. (highpeak
  post-march-2025 *is* company-wide — reported as such above.)
- **pixel wobble**: all cases survive the 750m check (no declaration on any
  lease, oil or gas id, within a full VIIRS pixel).

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
- the gas-id linkage is by (district, operator, lease name) — gas wells whose
  pdq name differs from the surface lease's name still slip through, and a
  same-named gas unit elsewhere in the district can pull in unrelated
  production. some undeclared fire is plausibly legal-but-misfiled on an
  entirely unrelated lease id — that is still a data integrity finding, just
  a different headline.
- compressor stations remain in neither plant list; the aggregate buckets
  surely retain some midstream contamination even though the named cases were
  individually cleared (see case validation).

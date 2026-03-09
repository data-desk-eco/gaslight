---
name: kpler
description: Use when fetching oil/gas trade flow data from Kpler. Covers authentication, trade queries, flow aggregations, entity search, vessel positions, and company fleet data.
---

# Kpler Trade Data API

Access oil and gas trade flows, vessel tracking, and company data from the Kpler terminal.

## Prerequisites

1. **Kpler account** with terminal access
2. Store credentials in `.env`:

```bash
KPLER_USERNAME=user@example.com
KPLER_PASSWORD=your_password
```

## Setup

Copy the client module to your project:

```bash
cp .claude/skills/kpler/scripts/kpler_client.py scripts/
```

Install dependencies:

```bash
uv add httpx pyjwt python-dotenv
```

## Quick start

```python
import asyncio
from kpler_client import KplerClient

async def main():
    async with KplerClient() as client:
        # Search for a player
        results = await client.search("shell", categories=["PLAYER"])
        shell_id = results["players"][0]["entity"]["id"]

        # Query their trades
        trades = await client.query_trades(players=[shell_id], size=20)
        print(f"Found {len(trades['result']['trades'])} trades")

asyncio.run(main())
```

## Authentication

The client handles Auth0 OAuth automatically:

- Auto-login from `.env` credentials
- Token storage in `.kpler_token`, `.kpler_refresh_token`
- Automatic refresh 5 minutes before expiry
- Retry on 401 with fresh token

Manual auth control:

```python
client = KplerClient()
await client.login("user@example.com", "password")  # Manual login
client.is_authenticated()  # Check status
await client.logout()  # Clear tokens
```

## API methods

### Search entities

Find players, vessels, installations, zones, and products by name:

```python
results = await client.search(
    text="gazprom",
    categories=["PLAYER", "INSTALLATION"],  # Optional filter
    commodity_types=["lng"],  # Optional: lng, oil, lpg, dry
)

# Results grouped by type
for player in results.get("players", []):
    print(f"{player['entity']['name']} (ID: {player['entity']['id']})")
```

**Categories:** `PLAYER`, `VESSEL`, `INSTALLATION`, `ZONE`, `PRODUCT`

### Query trades

Get individual trade records:

```python
trades = await client.query_trades(
    # Pagination
    from_=0,
    size=100,

    # Filters (use IDs from search - strings work, converted to ints)
    locations=[1234],  # Zone/installation IDs
    products=[5678],   # Product IDs
    players=[3836],    # Company IDs
    vessels=[9012],    # Vessel IDs

    # Trade status
    statuses=["completed"],  # ongoing, completed, cancelled
    trade_types=["import", "export"],

    # Options
    with_forecasted=False,
)

for trade in trades["data"]:
    origin = trade.get("portCallOrigin", {}).get("zone", {}).get("name", "Unknown")
    dest = trade.get("portCallDestination", {}).get("zone", {}).get("name", "Unknown")
    print(f"{origin} → {dest}")
```

### Query flows (aggregated)

Get aggregated flow data with time series:

```python
flows = await client.query_flows(
    # Required
    direction="export",  # export, import
    granularity="months",  # years, months, weeks, days

    # Date range
    start_date="2024-01-01",
    end_date="2024-12-31",

    # Filters
    locations=[1234],
    products=[5678],
    players=[3836],

    # Split results by dimension
    split_on="countries",  # countries, ports, products, vessels, buyers, sellers

    # Options
    cumulative=False,
    forecasted=False,
    intra=False,  # Include intra-region flows
)

# Response format: series by date with split values
for entry in flows["series"]:
    year = entry["date"]
    for dataset in entry.get("datasets", []):
        for split in dataset.get("splitValues", []):
            vol = split["values"]["volume"]
            print(f"{year}: {split['name']} = {vol/1e6:.1f} Mt")
```

**Split options:** `countries`, `ports`, `products`, `vessels`, `buyers`, `sellers`, `charterers`

### Get vessel positions

Raw AIS tracking data:

```python
positions = await client.get_vessel_positions(
    vessel_id=12345,
    start_date="2024-01-01T00:00:00Z",  # ISO 8601
    end_date="2024-01-31T23:59:59Z",
    limit=1000,
)

for pos in positions:
    print(f"{pos['timestamp']}: {pos['lat']}, {pos['lon']}")
```

### Get player fleet

Company fleet information:

```python
fleet = await client.get_player_fleet(player_id=3836)

print(f"Company: {fleet['name']}")
print(f"Vessels owned: {len(fleet.get('ownedVessels', []))}")
print(f"Subsidiaries: {len(fleet.get('subsidiaries', []))}")
```

### Query contracts

Long-term agreements and tenders:

```python
contracts = await client.query_contracts(
    types=["SPA", "LTA"],  # Tender, SPA, LTA, TUA
    players=[3836],
    from_=0,
    size=50,
)
```

## ETL pattern for notebooks

Typical workflow for building a DuckDB database:

```python
# scripts/fetch_kpler.py
import asyncio
import duckdb
from kpler_client import KplerClient
from dotenv import load_dotenv

load_dotenv()

async def fetch_data():
    async with KplerClient() as client:
        # Search for entities
        russia = await client.search("russia", categories=["ZONE"])
        russia_id = russia["zones"][0]["entity"]["id"]

        # Get export flows
        flows = await client.query_flows(
            direction="export",
            locations=[russia_id],
            granularity="months",
            start_date="2020-01-01",
            end_date="2024-12-31",
            split_on="countries",
        )

        return flows["result"]["series"]

def build_database(data):
    con = duckdb.connect("data/data.duckdb")
    con.execute("""
        CREATE OR REPLACE TABLE flows (
            date DATE,
            destination VARCHAR,
            volume_kt DOUBLE
        )
    """)

    for series in data:
        for point in series.get("data", []):
            con.execute(
                "INSERT INTO flows VALUES (?, ?, ?)",
                [point["date"], series["name"], point["value"]]
            )

    con.close()

if __name__ == "__main__":
    data = asyncio.run(fetch_data())
    build_database(data)
```

Add to Makefile:

```makefile
data:
	uv run python scripts/fetch_kpler.py
```

## Rate limits

The Kpler API has rate limits. Add delays for large queries:

```python
import asyncio

for player_id in player_ids:
    data = await client.query_trades(players=[player_id])
    await asyncio.sleep(0.5)  # Rate limit protection
```

## Common entity IDs

Search to find current IDs, as these may change:

| Entity | Example search | Type | ID |
|--------|----------------|------|----|
| Russia | `russia` | ZONE | |
| China | `china` | ZONE | |
| Shell | `shell` | PLAYER | |
| TotalEnergies | `total` | PLAYER | |
| Crude oil | `crude` | PRODUCT | |
| LNG | `lng` | PRODUCT | |
| F-76 Military Diesel | `F-76` | PRODUCT | 1484 |
| Jet JP5 | `JP-5` | PRODUCT | 1650 |
| Jet JP8 | `JP-8` | PRODUCT | 1652 |
| U.S. Navy | `navy` | PLAYER | 4224 |
| MSC (Military Sealift Command) | `MSC` | PLAYER | 3190 |

## Tracking military fuel shipments

Military fuel shipments can be identified through three complementary approaches: by product grade, by destination installation, and by buyer. Combine these for comprehensive coverage, as not all military shipments are labelled with military-specific product grades.

### Approach 1: Military-specific product grades

Kpler tracks these military fuel grades directly:

| Product | ID | Type | Parent | NATO code | Use |
|---------|-----|------|--------|-----------|-----|
| F-76 Military Diesel | 1484 | GRADE | Diesel (1394) | F-76 | Naval distillate fuel |
| Jet JP5 | 1650 | GRADE | Jet (1644) | F-44 | Naval aviation fuel (high flash point) |
| Jet JP8 | 1652 | GRADE | Jet (1644) | F-34 | Air force aviation fuel |

These sit within the product hierarchy: Commodities > Liquids > Clean > Clean Products > Middle Distillates > Gasoil/Diesel or Kero/Jet.

**Note:** Many military shipments are classified under broader categories (Jet A-1, Gasoil, Diesel, Marine Diesel) rather than military-specific grades. Cross-reference with destination and buyer for better identification.

Other relevant product IDs for broader searches:

| Product | ID | Type | Why relevant |
|---------|-----|------|-------------|
| Jet | 1644 | PRODUCT | Parent of all jet fuels |
| Jet A-1 | 1646 | GRADE | Standard military jet fuel in practice |
| Diesel | 1394 | PRODUCT | Parent of F-76 |
| Marine Diesel | 1806 | GRADE | Naval vessel fuel |
| Gasoil | 1510 | PRODUCT | Often used for military diesel |
| Kerosene | 1670 | PRODUCT | Heating/aviation overlap |
| Kero/Jet | 1672 | GROUP | Umbrella for all jet/kero |
| Gasoil/Diesel | 1530 | GROUP | Umbrella for all diesel/gasoil |

### Approach 2: Military installations

These installations are explicitly military or serve as known military fuel depots. Use their IDs in the `locations` parameter for `query_trades()`.

**US Navy (Pacific):**

| Installation | ID | Port (zone ID) | Type |
|-------------|-----|----------------|------|
| US Navy CFAY Yokosuka | 3025 | Yokosuka (3028) | Clean Products Consumption |
| US Navy Fuel Storage Akasaki | 3036 | Sasebo (3535) | Oil Product Import Terminal |
| US Navy Fuel Storage Akasaki 2 | 3035 | Sasebo (3535) | Oil Product Import Terminal |
| US Navy Yokose Filling Station | 12024 | Sasebo (3535) | Multi-Purpose Import Terminal |
| US Navy Okinawa | 3214 | Okinawa (2530) | Clean Products Import Terminal |
| US Navy White Beach Port Facility | 11006 | Okinawa (2530) | Multi-Purpose Import Terminal |
| US Naval Diego Garcia | 3201 | Diego Garcia (3632) | Oil Products Consumption |
| Naval Air Key West | 9761 | Key West | Multi-Purpose Import Terminal |
| Shell Guam | 2162 | Guam Port (1578) | Clean Products Consumption |

**UK MOD:**

| Installation | ID | Port (zone ID) | Type |
|-------------|-----|----------------|------|
| Loch Ewe | 10287 | Loch Ewe (113001) | Multi-Purpose Import Terminal |
| Loch Striven OFD | 3282 | Ardyne Point | Oil Product Import Terminal |
| Thanckes Oil Fuel Depot | 11723 | Thanckes | Multi-Purpose Import Terminal |
| Gosport | 10670 | Portsmouth UK (112940) | Multi-Purpose Import Terminal |
| Faslane Fuel Depot | 10954 | Faslane | Multi-Purpose Import Terminal |
| Devonport | 6193 | Devonport (6241) | Oil Product Import Terminal |

**NATO / Allied:**

| Installation | ID | Port (zone ID) | Type |
|-------------|-----|----------------|------|
| NATO Souda | 3090 | Souda Port (3553) | Oil Import Terminal |
| Rota Base | 3236 | Cadiz | Oil Product Import Terminal |
| Brest Base sous-marine | 13189 | Brest | Multi-Purpose Import Terminal |
| Praia da Vitoria | 4368 | Cabo Da Praia | LPG Consumption |
| Nordic Storage Campbeltown | 3259 | Campbeltown Port | Clean Products Consumption |

### Approach 3: Military buyers/players

| Player | ID | Notes |
|--------|-----|-------|
| U.S. Navy | 4224 | Primary US military buyer |
| MSC (Military Sealift Command) | 3190 | US Navy logistics arm |
| Defence International | 2312 | |

### Observed supply patterns

From trade data analysis, these patterns emerge:

**US Pacific operations:** S-Oil Onsan (Ulsan), SK Ulsan, GS Caltex Yeosu, and KPX Global in South Korea supply F-76 and JP-5 to US Navy bases across the Pacific (Yokosuka, Sasebo, Okinawa, Guam, Diego Garcia, Subic Bay).

**UK/NATO European operations:** MOH Corinth Refinery (Greece) and Gibraltar-San Roque Refinery supply F-76 and JP-5 to UK depots (Thanckes, Loch Striven, Loch Ewe) and NATO bases (Souda, Rota). Loch Ewe also receives Marine Diesel and Gasoil from NW Europe (Antwerp, Rotterdam, Skagen).

**French Navy:** Le Havre (Pointe du Hoc, ExxonMobil Port Jerome), Amsterdam (Exolum), Barcelona, and Antwerp supply F-76 to Brest Base sous-marine.

**JP-8 to Israel:** Valero Bill E (Corpus Christi) and Valero Port Arthur are recurring origins for JP-8 shipments to Ashkelon.

### Example: fetch all military fuel trades

```python
MILITARY_PRODUCT_IDS = [1484, 1650, 1652]  # F-76, JP-5, JP-8

MILITARY_INSTALLATION_IDS = [
    3025, 3036, 3035, 12024, 3214, 11006, 3201, 9761,  # US Navy
    10287, 3282, 11723, 10670, 10954, 6193,              # UK MOD
    3090, 3236, 13189, 4368, 3259,                        # NATO/Allied
]

MILITARY_PLAYER_IDS = [4224, 3190]  # U.S. Navy, MSC

async def fetch_military_trades(client):
    """Fetch trades identifiable as military by product, destination, or buyer."""
    all_trades = []

    # By military product grades
    for product_id in MILITARY_PRODUCT_IDS:
        trades = await client.query_trades(products=[product_id], size=200)
        all_trades.extend(trades.get("data", []))
        await asyncio.sleep(0.5)

    # By military installations (catches shipments with generic product labels)
    for install_id in MILITARY_INSTALLATION_IDS:
        trades = await client.query_trades(
            to_locations=[{"id": install_id, "resourceType": "installation"}],
            size=200,
        )
        all_trades.extend(trades.get("data", []))
        await asyncio.sleep(0.5)

    # By military buyer
    for player_id in MILITARY_PLAYER_IDS:
        trades = await client.query_trades(players=[player_id], size=200)
        all_trades.extend(trades.get("data", []))
        await asyncio.sleep(0.5)

    # Deduplicate by trade ID
    seen = set()
    unique = []
    for t in all_trades:
        origin = t.get("forecastPortCallOrigin") or {}
        tid = origin.get("id")
        if tid and tid not in seen:
            seen.add(tid)
            unique.append(t)

    return unique
```

### Example: aggregate military flows over time

```python
async def military_flow_timeseries(client, start="2020-01-01", end="2025-12-31"):
    """Get monthly military fuel flows split by destination."""
    flows = await client.query_flows(
        direction="import",
        granularity="months",
        start_date=start,
        end_date=end,
        products=[1484, 1650, 1652],  # F-76, JP-5, JP-8
        split_on="destination ports",
        number_of_splits=20,
    )
    return flows
```

### Limitations

- Many military shipments use generic product labels (Gasoil, Diesel, Jet A-1) so product-based filtering alone will miss a significant share
- Installation-based filtering catches more but may include some commercial activity at dual-use ports
- Ship-to-ship transfers and underway replenishment (UNREP) operations may not appear in trade data
- Faslane installation (10954) returned no trades despite existing in search; may require querying via the port zone instead

## Troubleshooting

**401 Unauthorized:** Credentials invalid or expired. Check `.env` file.

**Empty results:** Verify entity IDs are correct. Use `search()` to find current IDs.

**Token errors:** Delete `.kpler_token*` files and re-authenticate.

**Rate limited:** Add delays between requests. The API may temporarily block rapid queries.

## API reference

Base URL: `https://terminal.kpler.com/api`

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/trades` | POST | Query trade records |
| `/flows` | POST | Aggregate flow data |
| `/contracts` | GET | Contract data |
| `/players/{id}` | GET | Company fleet info |
| `/vessels/{id}/positions` | GET | AIS positions |
| `/graphql/` | POST | Entity search |

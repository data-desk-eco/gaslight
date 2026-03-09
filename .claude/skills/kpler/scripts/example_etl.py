#!/usr/bin/env python3
"""Example ETL script for fetching Kpler data into a DuckDB database.

Copy this file to your project's scripts/ directory and modify as needed.
Also copy kpler_client.py to the same directory.

Usage:
    uv run python scripts/example_etl.py

Prerequisites:
    - uv add httpx pyjwt python-dotenv duckdb
    - Create .env with KPLER_USERNAME and KPLER_PASSWORD
"""

import asyncio
from pathlib import Path

import duckdb
from dotenv import load_dotenv

from kpler_client import KplerClient

# Load environment variables
load_dotenv()

# Output database path
DB_PATH = Path("data/data.duckdb")


async def fetch_flows(client: KplerClient, zone_name: str) -> list[dict]:
    """Fetch export flows for a zone."""
    # First search for the zone ID
    results = await client.search(zone_name, categories=["ZONE"])
    if not results.get("zones"):
        print(f"Zone not found: {zone_name}")
        return []

    zone = results["zones"][0]["entity"]
    zone_id = zone["id"]  # String from search, converted to int by client
    print(f"Found zone: {zone['name']} (ID: {zone_id})")

    # Query flows
    flows = await client.query_flows(
        direction="export",
        locations=[zone_id],
        granularity="months",
        start_date="2020-01-01",
        end_date="2024-12-31",
        split_on="destination countries",
        number_of_splits=20,
    )

    return flows.get("series", [])


async def fetch_trades(client: KplerClient, player_name: str, limit: int = 100) -> list[dict]:
    """Fetch recent trades for a player/company."""
    # Search for the player
    results = await client.search(player_name, categories=["PLAYER"])
    if not results.get("players"):
        print(f"Player not found: {player_name}")
        return []

    player = results["players"][0]["entity"]
    player_id = player["id"]  # String from search, converted to int by client
    print(f"Found player: {player['name']} (ID: {player_id})")

    # Query trades
    trades = await client.query_trades(
        players=[player_id],
        size=limit,
        with_forecasted=False,
    )

    return trades.get("data", [])


def init_database(con: duckdb.DuckDBPyConnection):
    """Create database tables."""
    con.execute("""
        CREATE OR REPLACE TABLE flows (
            source_zone VARCHAR,
            destination VARCHAR,
            date DATE,
            volume_kt DOUBLE
        )
    """)

    con.execute("""
        CREATE OR REPLACE TABLE trades (
            trade_id VARCHAR,
            origin VARCHAR,
            destination VARCHAR,
            product VARCHAR,
            volume_kt DOUBLE,
            status VARCHAR,
            vessel_name VARCHAR,
            departure_date DATE
        )
    """)


def insert_flows(con: duckdb.DuckDBPyConnection, source_zone: str, series: list[dict]):
    """Insert flow data into database."""
    for entry in series:
        date = entry.get("date")
        for dataset in entry.get("datasets", []):
            for split in dataset.get("splitValues", []):
                destination = split.get("name", "Unknown")
                volume = split.get("values", {}).get("volume", 0)
                con.execute(
                    "INSERT INTO flows VALUES (?, ?, ?, ?)",
                    [source_zone, destination, date, volume / 1000],  # Convert to kt
                )


def insert_trades(con: duckdb.DuckDBPyConnection, trades: list[dict]):
    """Insert trade data into database."""
    for trade in trades:
        origin_pc = trade.get("portCallOrigin") or {}
        dest_pc = trade.get("portCallDestination") or {}
        origin = origin_pc.get("zone", {}).get("name", "Unknown")
        destination = dest_pc.get("zone", {}).get("name", "Unknown")

        # Get first commodity type as product
        commodities = trade.get("commodityTypes", [])
        product = commodities[0] if commodities else "Unknown"

        # Get volume from flow quantities
        flow_qty = trade.get("flowQuantityFromOrigin") or {}
        volume = flow_qty.get("mass")  # in tonnes

        status = trade.get("status", "Unknown")

        # Get first vessel name
        vessels = trade.get("vessels", [])
        vessel = vessels[0].get("name", "Unknown") if vessels else "Unknown"

        departure = trade.get("start")

        con.execute(
            "INSERT INTO trades VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                trade.get("id"),
                origin,
                destination,
                product,
                volume,
                status,
                vessel,
                departure,
            ],
        )


async def main():
    """Main ETL function."""
    # Ensure data directory exists
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Connect to database
    con = duckdb.connect(str(DB_PATH))
    init_database(con)

    async with KplerClient() as client:
        # Example: Fetch Russian export flows
        print("\nFetching Russia export flows...")
        russia_flows = await fetch_flows(client, "russia")
        if russia_flows:
            insert_flows(con, "Russia", russia_flows)
            print(f"  Inserted {len(russia_flows)} flow series")

        # Add delay to avoid rate limiting
        await asyncio.sleep(1)

        # Example: Fetch Shell trades
        print("\nFetching Shell trades...")
        shell_trades = await fetch_trades(client, "shell", limit=50)
        if shell_trades:
            insert_trades(con, shell_trades)
            print(f"  Inserted {len(shell_trades)} trades")

    # Print summary
    print("\nDatabase summary:")
    print(f"  Flows: {con.execute('SELECT COUNT(*) FROM flows').fetchone()[0]} records")
    print(f"  Trades: {con.execute('SELECT COUNT(*) FROM trades').fetchone()[0]} records")

    con.close()
    print(f"\nDatabase saved to {DB_PATH}")


if __name__ == "__main__":
    asyncio.run(main())

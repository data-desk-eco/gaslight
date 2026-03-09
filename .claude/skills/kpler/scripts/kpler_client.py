"""Standalone Kpler API client for Data Desk research notebooks.

Copy this file to your project's scripts/ directory and use it in ETL scripts
to fetch trade flow data from the Kpler terminal.

Usage:
    import asyncio
    from kpler_client import KplerClient

    async def main():
        async with KplerClient() as client:
            results = await client.search("shell", categories=["PLAYER"])
            print(results)

    asyncio.run(main())

Prerequisites:
    - pip install httpx pyjwt python-dotenv
    - Create .env file with KPLER_USERNAME and KPLER_PASSWORD
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import httpx
import jwt
from dotenv import load_dotenv


class KplerClient:
    """Client for interacting with Kpler API.

    Handles authentication, token refresh, and API requests.
    """

    # Auth configuration
    AUTH_URL = "https://auth.kpler.com/oauth/token"
    CLIENT_ID = "0LglhXfJvfepANl3HqVT9i1U0OwV0gSP"
    AUDIENCE = "https://terminal.kpler.com"
    AUTH0_CLIENT = "eyJuYW1lIjoiYXV0aDAtc3BhLWpzIiwidmVyc2lvbiI6IjIuMS4zIn0="

    # API configuration
    BASE_URL = "https://terminal.kpler.com/api"
    GRAPHQL_URL = "https://terminal.kpler.com/graphql/"
    WEB_VERSION = "v21.2161.1"

    def __init__(self, token_dir: Path | str | None = None):
        """Initialize client.

        Args:
            token_dir: Directory to store tokens. Defaults to current directory.
        """
        load_dotenv()
        self.token_dir = Path(token_dir) if token_dir else Path.cwd()
        self.token_file = self.token_dir / ".kpler_token"
        self.refresh_token_file = self.token_dir / ".kpler_refresh_token"
        self.token_info_file = self.token_dir / ".kpler_token_info"
        self._http_client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "KplerClient":
        """Async context manager entry - auto-login from environment."""
        self._http_client = httpx.AsyncClient(timeout=30.0)
        if not self.is_authenticated():
            username = os.getenv("KPLER_USERNAME")
            password = os.getenv("KPLER_PASSWORD")
            if username and password:
                await self.login(username, password)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    def _decode_jwt(self, token: str) -> dict:
        """Decode JWT token without verification to extract expiry."""
        return jwt.decode(token, options={"verify_signature": False})

    def _save_tokens(self, access_token: str, refresh_token: str) -> None:
        """Save tokens to files."""
        self.token_file.write_text(access_token)
        self.refresh_token_file.write_text(refresh_token)
        token_data = self._decode_jwt(access_token)
        expires_at = token_data.get("exp", 0)
        self.token_info_file.write_text(str(expires_at))

    def _load_tokens(self) -> dict | None:
        """Load tokens from files."""
        if not all(
            f.exists()
            for f in [self.token_file, self.refresh_token_file, self.token_info_file]
        ):
            return None
        try:
            return {
                "access_token": self.token_file.read_text().strip(),
                "refresh_token": self.refresh_token_file.read_text().strip(),
                "expires_at": int(self.token_info_file.read_text().strip()),
            }
        except Exception:
            return None

    def _get_headers(self, access_token: str) -> dict[str, str]:
        """Get common request headers."""
        return {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "origin": "https://terminal.kpler.com",
            "referer": "https://terminal.kpler.com/",
            "use-access-token": "true",
            "x-access-token": access_token,
            "x-web-application-version": self.WEB_VERSION,
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/138.0.0.0 Safari/537.36"
            ),
        }

    async def login(self, username: str, password: str) -> tuple[bool, str]:
        """Perform initial login.

        Args:
            username: Kpler username
            password: Kpler password

        Returns:
            Tuple of (success, message)
        """
        headers = {
            "accept": "*/*",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://terminal.kpler.com",
            "referer": "https://terminal.kpler.com/",
            "auth0-client": self.AUTH0_CLIENT,
        }
        data = {
            "grant_type": "password",
            "username": username,
            "password": password,
            "client_id": self.CLIENT_ID,
            "audience": self.AUDIENCE,
            "scope": "openid profile email offline_access",
        }

        client = self._http_client or httpx.AsyncClient()
        try:
            response = await client.post(self.AUTH_URL, headers=headers, data=data)
            response.raise_for_status()
            result = response.json()
            access_token = result.get("access_token")
            refresh_token = result.get("refresh_token")
            if not access_token or not refresh_token:
                return False, "Invalid response: missing tokens"
            self._save_tokens(access_token, refresh_token)
            return True, "Login successful"
        except httpx.HTTPStatusError as e:
            error_msg = "Login failed"
            try:
                error_data = e.response.json()
                error_msg = error_data.get("error_description", error_msg)
            except Exception:
                pass
            return False, f"{error_msg}: {e.response.status_code}"
        except Exception as e:
            return False, f"Login error: {str(e)}"
        finally:
            if not self._http_client:
                await client.aclose()

    async def _refresh_token(self) -> tuple[bool, str]:
        """Refresh access token using refresh token."""
        token_info = self._load_tokens()
        if not token_info:
            return False, "No refresh token found"

        headers = {
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://terminal.kpler.com",
            "referer": "https://terminal.kpler.com/",
            "auth0-client": self.AUTH0_CLIENT,
        }
        data = {
            "grant_type": "refresh_token",
            "client_id": self.CLIENT_ID,
            "refresh_token": token_info["refresh_token"],
            "audience": self.AUDIENCE,
        }

        client = self._http_client or httpx.AsyncClient()
        try:
            response = await client.post(self.AUTH_URL, headers=headers, data=data)
            response.raise_for_status()
            result = response.json()
            access_token = result.get("access_token")
            new_refresh_token = result.get(
                "refresh_token", token_info["refresh_token"]
            )
            if not access_token:
                return False, "Invalid response: missing access token"
            self._save_tokens(access_token, new_refresh_token)
            return True, "Token refreshed successfully"
        except httpx.HTTPStatusError as e:
            error_msg = "Token refresh failed"
            try:
                error_data = e.response.json()
                error_msg = error_data.get("error_description", error_msg)
            except Exception:
                pass
            return False, f"{error_msg}: {e.response.status_code}"
        except Exception as e:
            return False, f"Refresh error: {str(e)}"
        finally:
            if not self._http_client:
                await client.aclose()

    async def _get_valid_token(self) -> str | None:
        """Get a valid access token, refreshing if necessary."""
        token_info = self._load_tokens()
        if not token_info:
            return None

        # Refresh if token expires in next 5 minutes
        current_time = int(time.time())
        if current_time > (token_info["expires_at"] - 300):
            success, _ = await self._refresh_token()
            if not success:
                return None
            token_info = self._load_tokens()
            if not token_info:
                return None

        return token_info["access_token"]

    def logout(self) -> None:
        """Clear stored tokens."""
        for file in [self.token_file, self.refresh_token_file, self.token_info_file]:
            if file.exists():
                file.unlink()

    def is_authenticated(self) -> bool:
        """Check if we have valid tokens stored."""
        return self._load_tokens() is not None

    async def _request(
        self,
        method: str,
        url: str,
        json: dict | None = None,
        params: dict | None = None,
    ) -> dict[str, Any]:
        """Make an authenticated API request with automatic token refresh."""
        token = await self._get_valid_token()
        if not token:
            raise Exception("Not authenticated. Please login first.")

        headers = self._get_headers(token)
        client = self._http_client or httpx.AsyncClient(timeout=30.0)

        try:
            if method == "GET":
                response = await client.get(url, headers=headers, params=params)
            else:
                response = await client.post(url, headers=headers, json=json)

            # Handle 401 with token refresh
            if response.status_code == 401:
                success, _ = await self._refresh_token()
                if success:
                    token = await self._get_valid_token()
                    if token:
                        headers = self._get_headers(token)
                        if method == "GET":
                            response = await client.get(
                                url, headers=headers, params=params
                            )
                        else:
                            response = await client.post(
                                url, headers=headers, json=json
                            )

            response.raise_for_status()
            return response.json()

        except httpx.HTTPStatusError as e:
            error_msg = f"API request failed: {e.response.status_code}"
            try:
                error_data = e.response.json()
                error_msg = f"{error_msg} - {error_data}"
            except Exception:
                error_msg = f"{error_msg} - {e.response.text[:200]}"
            raise Exception(error_msg) from e
        except Exception as e:
            raise Exception(f"Request error: {str(e)}") from e
        finally:
            if not self._http_client:
                await client.aclose()

    async def search(
        self,
        text: str,
        categories: list[str] | None = None,
        commodity_types: list[str] | None = None,
        product_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """Search for entities (players, vessels, zones, etc).

        Args:
            text: Search text
            categories: Entity categories (PLAYER, VESSEL, INSTALLATION, ZONE, PRODUCT)
            commodity_types: Commodity types (lng, oil, lpg, dry)
            product_ids: Product IDs to filter by

        Returns:
            Results organized by type: zones, installations, players, products, vessels
        """
        if categories is None:
            categories = ["INSTALLATION", "ZONE", "PLAYER", "VESSEL", "PRODUCT"]

        options = {}
        if commodity_types:
            options["installation"] = {"commodityTypes": commodity_types}
            options["vessel"] = {"commodityTypes": commodity_types}
        if product_ids:
            options["product"] = {"ids": product_ids}

        query = """query searchCompletion(
            $text: String!,
            $options: SearchOptionsInput,
            $category: [SearchCategory!]!
        ) {
  completionSearch(text: $text, category: $category, options: $options) {
    ...zoneSearch
    ...installationSearch
    ...playerSearch
    ...productSearch
    ...vesselSearch
    __typename
  }
}

fragment zoneSearch on ZoneSearch {
  highlight { field value __typename }
  score
  zone { id hasStorage isInUnitedStates name type unlocodes __typename }
  __typename
}

fragment installationSearch on InstallationSearch {
  highlight { field value __typename }
  installation {
    hasCargoTracking hasStorage id isInUnitedStates name iirName
    port { id __typename }
    portCost type unlocodes commodityTypes __typename
  }
  score
  __typename
}

fragment playerSearch on PlayerSearch {
  score
  highlight { field value __typename }
  player { id name __typename }
  __typename
}

fragment productSearch on ProductSearch {
  score
  highlight { field value __typename }
  product { id name type: typeEnum hasEstimation parentId __typename }
  __typename
}

fragment vesselSearch on VesselSearch {
  score
  highlight { field value __typename }
  vessel { id name status imo hasLastPosition currentCommodityType __typename }
  __typename
}"""

        payload = {
            "operationName": "searchCompletion",
            "variables": {"category": categories, "text": text, "options": options},
            "query": query,
        }

        token = await self._get_valid_token()
        if not token:
            raise Exception("Not authenticated. Please login first.")

        headers = self._get_headers(token)
        headers.update(
            {
                "apollographql-client-name": "Web",
                "apollographql-client-version": self.WEB_VERSION,
            }
        )

        client = self._http_client or httpx.AsyncClient(timeout=30.0)
        try:
            response = await client.post(
                self.GRAPHQL_URL, headers=headers, json=payload
            )
            response.raise_for_status()
            result = response.json()

            if "data" in result and "completionSearch" in result["data"]:
                search_results = result["data"]["completionSearch"]
                organized = {
                    "zones": [],
                    "installations": [],
                    "players": [],
                    "products": [],
                    "vessels": [],
                }

                type_map = {
                    "ZoneSearch": ("zones", "zone"),
                    "InstallationSearch": ("installations", "installation"),
                    "PlayerSearch": ("players", "player"),
                    "ProductSearch": ("products", "product"),
                    "VesselSearch": ("vessels", "vessel"),
                }

                for item in search_results:
                    typename = item.get("__typename")
                    if typename in type_map:
                        key, entity_key = type_map[typename]
                        organized[key].append(
                            {
                                "score": item["score"],
                                "highlight": item.get("highlight"),
                                "entity": item[entity_key],
                            }
                        )

                return organized

            return result
        finally:
            if not self._http_client:
                await client.aclose()

    async def query_trades(
        self,
        from_: int = 0,
        size: int = 40,
        locations: list[int] | None = None,
        from_locations: list[dict[str, Any]] | None = None,
        to_locations: list[dict[str, Any]] | None = None,
        vessels: list[int] | None = None,
        products: list[int] | None = None,
        players: list[int] | None = None,
        statuses: list[str] | None = None,
        trade_types: list[str] | None = None,
        with_forecasted: bool = True,
    ) -> dict[str, Any]:
        """Query trade records.

        Args:
            from_: Starting offset for pagination
            size: Number of results to return
            locations: Location IDs to filter by
            from_locations: Origin locations (e.g., [{"id": 123, "resourceType": "zone"}])
            to_locations: Destination locations
            vessels: Vessel IDs to filter by
            products: Product IDs to filter by
            players: Player/company IDs to filter by
            statuses: Trade status filter (ongoing, completed, cancelled)
            trade_types: Trade type filter (import, export)
            with_forecasted: Include forecasted trades

        Returns:
            API response with trades
        """
        # Convert simple location IDs to full format (ensure IDs are ints)
        loc_list = []
        if locations:
            loc_list = [{"id": int(loc), "resourceType": "zone"} for loc in locations]

        # Ensure all IDs are integers
        vessel_ids = [int(v) for v in vessels] if vessels else []
        product_ids = [int(p) for p in products] if products else []
        player_ids = [int(p) for p in players] if players else []

        payload = {
            "from": from_,
            "size": size,
            "withForecasted": with_forecasted,
            "withProductEstimation": False,
            "locations": loc_list,
            "subLocations": [],
            "fromLocations": from_locations or [],
            "toLocations": to_locations or [],
            "vessels": vessel_ids,
            "products": product_ids,
            "players": player_ids,
            "status": statuses or [],
            "tradeTypes": trade_types or [],
        }

        result = await self._request("POST", f"{self.BASE_URL}/trades", json=payload)
        # API returns list directly, wrap for consistent access
        if isinstance(result, list):
            return {"data": result, "totalCount": len(result)}
        return result

    async def query_flows(
        self,
        direction: str = "export",
        granularity: str = "months",
        start_date: str = "2020-01-01",
        end_date: str = "2024-12-31",
        locations: list[int] | None = None,
        from_locations: list[dict[str, Any]] | None = None,
        to_locations: list[dict[str, Any]] | None = None,
        products: list[int] | None = None,
        players: list[int] | None = None,
        vessels: list[int] | None = None,
        split_on: str = "destination countries",
        number_of_splits: int = 10,
        cumulative: bool = False,
        forecasted: bool = True,
        intra: bool = False,
    ) -> dict[str, Any]:
        """Query aggregated flow data.

        Args:
            direction: Flow direction (export, import)
            granularity: Time granularity (years, months, weeks, days)
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            locations: Location IDs for filtering
            from_locations: Origin locations
            to_locations: Destination locations
            products: Product IDs
            players: Player/company IDs
            vessels: Vessel IDs
            split_on: Dimension to split by (destination countries, ports, products, etc)
            number_of_splits: Number of splits to return
            cumulative: Show cumulative data
            forecasted: Include forecasted data
            intra: Include intra-region flows

        Returns:
            API response with flow data
        """
        # Convert simple location IDs to full format (ensure IDs are ints)
        from_locs = from_locations or []
        to_locs = to_locations or []
        if locations:
            from_locs = [{"id": int(loc), "resourceType": "zone"} for loc in locations]

        # Build filters (ensure IDs are ints)
        filters = {}
        if products:
            filters["product"] = [int(p) for p in products]
        if players:
            filters["player"] = [int(p) for p in players]

        # Ensure vessel IDs are ints
        vessel_ids = [int(v) for v in vessels] if vessels else []

        payload = {
            "cumulative": cumulative,
            "filters": filters,
            "flowDirection": direction,
            "fromLocations": from_locs,
            "toLocations": to_locs,
            "fromLocationsExclude": [],
            "toLocationsExclude": [],
            "viaRoute": [],
            "granularity": granularity,
            "interIntra": "interintra" if intra else "inter",
            "onlyRealized": False,
            "withBetaVessels": False,
            "withForecasted": forecasted,
            "withGrades": False,
            "withIncompleteTrades": True,
            "withIntraCountry": intra,
            "withProductEstimation": False,
            "vessels": vessel_ids,
            "splitOn": split_on,
            "startDate": start_date,
            "endDate": end_date,
            "numberOfSplits": number_of_splits,
        }

        return await self._request("POST", f"{self.BASE_URL}/flows", json=payload)

    async def query_contracts(
        self,
        from_: int = 0,
        size: int = 200,
        types: list[str] | None = None,
        players: list[int] | None = None,
    ) -> dict[str, Any]:
        """Query contract data.

        Args:
            from_: Starting offset
            size: Number of results
            types: Contract types (Tender, SPA, LTA, TUA)
            players: Player IDs to filter by

        Returns:
            API response with contracts
        """
        params = {"from": from_, "size": size}
        if types:
            params["types"] = ",".join(types)
        if players:
            params["players"] = ",".join(map(str, players))

        return await self._request(
            "GET", f"{self.BASE_URL}/contracts", params=params
        )

    async def get_vessel_positions(
        self,
        vessel_id: int,
        start_date: str,
        end_date: str,
        limit: int = 5000,
    ) -> dict[str, Any]:
        """Get AIS positions for a vessel.

        Args:
            vessel_id: Vessel ID
            start_date: Start of time window (ISO 8601)
            end_date: End of time window (ISO 8601)
            limit: Maximum positions to return

        Returns:
            API response with position data
        """
        params = {"after": start_date, "before": end_date, "limit": limit}
        return await self._request(
            "GET", f"{self.BASE_URL}/vessels/{vessel_id}/positions", params=params
        )

    async def get_player_fleet(self, player_id: int) -> dict[str, Any]:
        """Get fleet information for a company.

        Args:
            player_id: Player/company ID

        Returns:
            API response with fleet details, owned vessels, subsidiaries, etc.
        """
        return await self._request("GET", f"{self.BASE_URL}/players/{player_id}")

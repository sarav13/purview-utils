import os
import re
import time
import pyodbc
import struct
import requests
import msal
import pandas as pd
from datetime import datetime
from urllib.parse import unquote
from azure.identity import ClientSecretCredential
from azure.purview.datamap import DataMapClient
from azure.core.exceptions import HttpResponseError
import dotenv

dotenv.load_dotenv()

# ──────────────────────────────────────────────────────────────────
# CONFIGURE HERE
# ──────────────────────────────────────────────────────────────────

TARGET_COLLECTION_ID = "xxxxx" #CollectionID
OUTPUT_DIR = "./purview_reports"
DRY_RUN = True


# ──────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────

class Config:
    def __init__(self):
        self.tenant_id = os.getenv("TENANTID")
        self.client_id = os.getenv("CLIENTID")
        self.client_secret = os.getenv("CLIENTSECRET")
        self.purview_account_name = os.getenv("PURVIEWACCOUNTNAME")
        self.purview_endpoint = os.getenv("PURVIEWENDPOINT")

        print("=== Config loaded ===")
        print(
            f"  TENANTID           : {'SET' if self.tenant_id            else '❌ MISSING'}")
        print(
            f"  CLIENTID           : {'SET' if self.client_id            else '❌ MISSING'}")
        print(
            f"  CLIENTSECRET       : {'SET' if self.client_secret        else '❌ MISSING'}")
        print(
            f"  PURVIEWACCOUNTNAME : {self.purview_account_name          or '❌ MISSING'}")
        print(
            f"  PURVIEWENDPOINT    : {self.purview_endpoint               or '❌ MISSING'}")


# ──────────────────────────────────────────────────────────────────
# PURVIEW CLIENT
# ──────────────────────────────────────────────────────────────────

class PurviewClient:
    def __init__(self, config: Config):
        self.config = config
        self.credential = ClientSecretCredential(
            config.tenant_id,
            config.client_id,
            config.client_secret
        )
        self.client = DataMapClient(
            endpoint=f"https://{config.purview_account_name}.purview.azure.com",
            credential=self.credential
        )

    def get_assets(self, collection_id: str) -> pd.DataFrame:
        df_list = []
        last_id = None
        total = 0

        print(f"\n=== Fetching assets from collection: '{collection_id}' ===")

        while True:
            filter_clause = {"and": [{"collectionId": collection_id}]}
            if last_id:
                filter_clause["and"].append(
                    {"id": {"operator": "gt", "value": last_id}}
                )

            body = {
                "keywords": "*",
                "limit":    1000,
                "filter":   filter_clause,
                "orderby":  [{"id": "asc"}]
            }

            try:
                res = self.client.discovery.query(body=body)
            except HttpResponseError as e:
                print(f"  ❌ Search error: {e}")
                break

            if not res or not res.get("value"):
                print("  No more results.")
                break

            batch = res["value"]
            last_id = batch[-1]["id"]
            total += len(batch)

            df_list.append(pd.DataFrame(batch))
            print(f"  Fetched batch: {len(batch)} | Running total: {total}")

            if len(batch) < 1000:
                break

        if not df_list:
            print("  ⚠️  No assets found.")
            return pd.DataFrame()

        df = pd.concat(df_list, ignore_index=True)
        print(f"  ✅ Total assets fetched: {len(df)}")
        print(f"  Columns: {df.columns.tolist()}")
        return df


# ──────────────────────────────────────────────────────────────────
# FABRIC CLIENT
# Resolves SQL endpoint hostname per lakehouse via Fabric REST API
# ──────────────────────────────────────────────────────────────────

class FabricClient:
    """
    Used ONLY to resolve:
        lakehouse_id → SQL endpoint connection string

    Fabric API:
        GET /v1/workspaces/{wsId}/lakehouses/{lhId}
        → .properties.sqlEndpointProperties.connectionString
        e.g. "xxxx.datawarehouse.fabric.microsoft.com"
    """

    API = "https://api.fabric.microsoft.com/v1"
    SCOPE = "https://analysis.windows.net/powerbi/api/.default"

    def __init__(self, config: Config):
        self.config = config
        self._token = None
        self._token_expiry = 0
        # cache: lakehouse_id → sql_server_hostname | None
        self._lh_cache: dict[str, str | None] = {}

    def _get_token(self) -> str:
        if self._token and time.time() < self._token_expiry - 60:
            return self._token
        cred = ClientSecretCredential(
            self.config.tenant_id,
            self.config.client_id,
            self.config.client_secret
        )
        tok = cred.get_token(self.SCOPE)
        self._token = tok.token
        self._token_expiry = tok.expires_on
        return self._token

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type":  "application/json"
        }

    def get_sql_endpoint(self, workspace_id: str, lakehouse_id: str) -> str | None:
        """
        Returns the SQL endpoint hostname for a given lakehouse.
        e.g. "abc123.datawarehouse.fabric.microsoft.com"

        Returns None if lakehouse deleted or not accessible.
        """
        cache_key = f"{workspace_id}:{lakehouse_id}"
        if cache_key in self._lh_cache:
            return self._lh_cache[cache_key]

        url = f"{self.API}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
        resp = requests.get(url, headers=self._headers())

        print(
            f"  [Fabric API] GET lakehouse {lakehouse_id} → HTTP {resp.status_code}")

        if resp.status_code == 200:
            data = resp.json()
            conn_str = (
                data
                .get("properties", {})
                .get("sqlEndpointProperties", {})
                .get("connectionString")
            )
            if conn_str:
                print(f"  ✅ SQL endpoint resolved: {conn_str}")
                self._lh_cache[cache_key] = conn_str
                return conn_str
            else:
                print(
                    f"  ⚠️  SQL endpoint not yet provisioned for lakehouse: {lakehouse_id}")
                print(f"      Full response: {data}")
                self._lh_cache[cache_key] = None
                return None

        elif resp.status_code == 404:
            print(f"  ❌ Lakehouse not found (404) — deleted: {lakehouse_id}")
            self._lh_cache[cache_key] = None
            return None

        elif resp.status_code == 403:
            print(
                f"  ⚠️  403 Forbidden for lakehouse: {lakehouse_id} — SP has no access")
            self._lh_cache[cache_key] = None
            return None

        else:
            print(
                f"  ❌ Unexpected {resp.status_code} for lakehouse {lakehouse_id}: {resp.text[:200]}")
            self._lh_cache[cache_key] = None
            return None


# ──────────────────────────────────────────────────────────────────
# SQL CLIENT
# Connects to Fabric SQL endpoint via AAD token (no password needed)
# ──────────────────────────────────────────────────────────────────

class SQLClient:
    """
    Connects to Fabric Lakehouse SQL endpoints using AAD token auth.
    Caches table lists per (server, database) to avoid repeat queries.
    """

    def __init__(self, config: Config):
        self.config = config
        # cache: "server::database" → set of (schema, table_name)
        self._cache: dict[str, set[tuple[str, str]]] = {}

    def _get_aad_token(self) -> str:
        """Acquire AAD token for Fabric SQL endpoint."""
        authority = f"https://login.microsoftonline.com/{self.config.tenant_id}"
        scope = ["https://database.windows.net/.default"]

        app = msal.ConfidentialClientApplication(
            self.config.client_id,
            client_credential=self.config.client_secret,
            authority=authority
        )
        result = app.acquire_token_for_client(scopes=scope)

        if "access_token" not in result:
            raise RuntimeError(
                f"AAD token for SQL failed: {result.get('error_description', result)}"
            )
        return result["access_token"]

    def _connect(self, server: str, database: str) -> pyodbc.Connection:
        """
        Open a pyodbc connection to a Fabric SQL endpoint using AAD token.
        server   : e.g. "abc123.datawarehouse.fabric.microsoft.com"
        database : lakehouse name (NOT the GUID — the display name)
        """
        token = self._get_aad_token()
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack("=i", len(token_bytes)) + token_bytes

        conn_str = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server=tcp:{server},1433;"
            f"Database={database};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        print(f"  Connecting to SQL: {server} / {database}")
        return pyodbc.connect(conn_str, attrs_before={1256: token_struct})

    def fetch_tables(self, server: str, database: str) -> set[tuple[str, str]]:
        """
        Returns set of (schema_name, table_name) — all lowercase.
        Includes both BASE TABLE and VIEW.
        Caches result per server+database.
        """
        cache_key = f"{server}::{database}"
        if cache_key in self._cache:
            print(f"  [SQL cache hit] {database}")
            return self._cache[cache_key]

        print(f"\n  Fetching tables from SQL endpoint: {server} / {database}")

        try:
            conn = self._connect(server, database)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            """)
            tables = {
                (row.TABLE_SCHEMA.lower(), row.TABLE_NAME.lower())
                for row in cursor.fetchall()
            }
            conn.close()
            print(f"  ✅ {len(tables)} tables/views found in '{database}'")
            self._cache[cache_key] = tables
            return tables

        except Exception as e:
            print(f"  ❌ SQL error for {server}/{database}: {e}")
            self._cache[cache_key] = set()
            return set()


# ──────────────────────────────────────────────────────────────────
# QN PARSER
# Handles double-encoded FQNs stored by Purview for Fabric assets
#
# Sample FQNs Purview stores:
#   workspace  : https://app.powerbi.com/groups/<wsId>
#   notebook   : https://app.fabric.microsoft.com/groups/<wsId>/synapsenotebooks/<itemId>
#   lakehouse  : https://app.fabric.microsoft.com/groups/<wsId>/lakehouses/<lhId>
#   sql endpt  : https://app.fabric.microsoft.com/groups/<wsId>/lakewarehouses/<itemId>
#   table      : https://app.fabric.microsoft.com/groups/<wsId>/lakehouses/<lhId>/tables/dbo%252Ftable_name
# ──────────────────────────────────────────────────────────────────

def normalize_qn(qn: str) -> str:
    """Double-decode percent-encoded characters in Purview FQNs."""
    if not qn:
        return ""
    decoded = unquote(unquote(qn.strip()))
    return decoded


def parse_qn(qn_raw: str) -> dict:
    """
    Parse a Purview qualifiedName into its components.

    Returns dict with keys:
        asset_type  : 'workspace' | 'notebook' | 'lakehouse' | 'sql_endpoint' | 'table' | 'unknown'
        workspace_id
        lakehouse_id
        item_id      (notebook / sql_endpoint item ID)
        schema       (for tables)
        table        (for tables)
        qn_decoded   (fully decoded FQN)
    """
    qn = normalize_qn(qn_raw)

    result = {
        "asset_type":   "unknown",
        "workspace_id": None,
        "lakehouse_id": None,
        "item_id":      None,
        "schema":       None,
        "table":        None,
        "qn_decoded":   qn,
    }

    # Workspace ID (groups or workspaces)
    ws_match = re.search(r"(?:groups|workspaces)/([0-9a-f-]{36})", qn, re.I)
    if ws_match:
        result["workspace_id"] = ws_match.group(1)

    # ── Workspace only (powerbi.com/groups/<id> with nothing after)
    if re.search(r"app\.powerbi\.com/groups/[0-9a-f-]{36}$", qn, re.I):
        result["asset_type"] = "workspace"
        return result

    # ── Notebook
    nb_match = re.search(r"synapsenotebooks/([0-9a-f-]{36})", qn, re.I)
    if nb_match:
        result["asset_type"] = "notebook"
        result["item_id"] = nb_match.group(1)
        return result

    # ── SQL endpoint (lakewarehouses)
    sq_match = re.search(r"lakewarehouses/([0-9a-f-]{36})", qn, re.I)
    if sq_match:
        result["asset_type"] = "sql_endpoint"
        result["item_id"] = sq_match.group(1)
        return result

    # ── Lakehouse (no /tables/ suffix)
    lh_match = re.search(r"lakehouses/([0-9a-f-]{36})", qn, re.I)
    if lh_match:
        result["lakehouse_id"] = lh_match.group(1)

        # ── Table / View under lakehouse
        tbl_match = re.search(r"/tables/([^/]+)/([^/]+)$", qn, re.I)
        if tbl_match:
            result["asset_type"] = "table"
            result["schema"] = tbl_match.group(1).lower()
            result["table"] = tbl_match.group(2).lower()
        else:
            result["asset_type"] = "lakehouse"

        return result

    return result


# ──────────────────────────────────────────────────────────────────
# ORPHAN DETECTION
# Strategy per asset type:
#   workspace    - skip (not a deletable asset in this context)
#   notebook     - skip (Fabric Items API check - out of scope here)
#   lakehouse    - Fabric API GET /lakehouses/{id} - 404 = orphan
#   sql_endpoint - tied to lakehouse - skip independently
#   table        - SQL INFORMATION_SCHEMA check via lakehouse SQL endpoint
# ──────────────────────────────────────────────────────────────────

def detect_orphans(
    df: pd.DataFrame,
    fabric_client: FabricClient,
    sql_client: SQLClient,
) -> pd.DataFrame:

    print(f"\n=== Orphan detection for {len(df)} assets ===")
    results = []

    # Cache: lakehouse_id → display_name (needed as DB name for SQL connect)
    lh_name_cache: dict[str, str | None] = {}

    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        name = row.get("name", "")
        qn_raw = str(row.get("qualifiedName", "")).strip()

        parsed = parse_qn(qn_raw)

        print(f"\n--- [{idx}/{len(df)}] {name} ---")
        print(f"  asset_type   : {parsed['asset_type']}")
        print(f"  workspace_id : {parsed['workspace_id']}")
        print(f"  lakehouse_id : {parsed['lakehouse_id']}")
        print(f"  schema       : {parsed['schema']}")
        print(f"  table        : {parsed['table']}")
        print(f"  qn_decoded   : {parsed['qn_decoded']}")

        ws_id = parsed["workspace_id"]
        lh_id = parsed["lakehouse_id"]
        atype = parsed["asset_type"]

        # ── Workspace — skip
        if atype == "workspace":
            print("  → SKIP (workspace-level asset)")
            results.append(("skip", "workspace_asset"))
            continue

        # ── Notebook — skip (handled separately if needed)
        if atype == "notebook":
            print("  → SKIP (notebook — not in scope)")
            results.append(("skip", "notebook_asset"))
            continue

        # ── SQL endpoint — tied to lakehouse, skip independently
        if atype == "sql_endpoint":
            print("  → SKIP (sql_endpoint — validated via lakehouse)")
            results.append(("skip", "sql_endpoint_asset"))
            continue

        # ── Unknown / unparsable
        if atype == "unknown" or not ws_id:
            print("  → SKIP (unparsable qualifiedName)")
            results.append(("skip", "unparsable_qn"))
            continue

        # ── Lakehouse — check via Fabric API
        if atype == "lakehouse":
            sql_server = fabric_client.get_sql_endpoint(ws_id, lh_id)
            if sql_server is None:
                print("  → ORPHAN ✅ (lakehouse deleted or inaccessible)")
                results.append(("orphan", "lakehouse_deleted"))
            else:
                print("  → CLEAN (lakehouse exists)")
                results.append(("clean", "lakehouse_exists"))
            continue

        # ── Table — validate via SQL INFORMATION_SCHEMA
        if atype == "table":
            schema = parsed["schema"]
            table = parsed["table"]

            # Step 1: resolve SQL endpoint for this lakehouse
            sql_server = fabric_client.get_sql_endpoint(ws_id, lh_id)

            if sql_server is None:
                print("  → ORPHAN ✅ (parent lakehouse deleted)")
                results.append(("orphan", "parent_lakehouse_deleted"))
                continue

            # Step 2: resolve lakehouse display name for DB connection
            # The SQL endpoint database name = lakehouse display name
            # We get this from the 'name' field of the lakehouse asset in df
            # OR from a separate Fabric API call
            if lh_id not in lh_name_cache:
                lh_asset = df[
                    (df.get("qualifiedName", pd.Series(dtype=str))
                     .apply(lambda q: lh_id in str(q)))
                    & (df.get("assetType", pd.Series(dtype=str))
                       .apply(lambda t: "lakehouse" in str(t).lower()
                              if t else False))
                ].head(1)

                if not lh_asset.empty:
                    lh_name_cache[lh_id] = lh_asset.iloc[0].get("name", "")
                    print(
                        f"  Lakehouse name (from df): {lh_name_cache[lh_id]}")
                else:
                    # Fallback: fetch from Fabric API
                    url = f"{FabricClient.API}/workspaces/{ws_id}/lakehouses/{lh_id}"
                    resp = requests.get(url, headers=fabric_client._headers())
                    if resp.status_code == 200:
                        lh_name_cache[lh_id] = resp.json().get(
                            "displayName", "")
                        print(
                            f"  Lakehouse name (from Fabric API): {lh_name_cache[lh_id]}")
                    else:
                        lh_name_cache[lh_id] = None
                        print(
                            f"  ⚠️  Could not resolve lakehouse name for {lh_id}")

            lh_name = lh_name_cache.get(lh_id)

            if not lh_name:
                print("  → SKIP (could not resolve lakehouse display name)")
                results.append(("skip", "lh_name_unresolved"))
                continue

            # Step 3: query SQL endpoint
            tables = sql_client.fetch_tables(sql_server, lh_name)

            if (schema, table) in tables:
                print(f"  → CLEAN ✅ ({schema}.{table} exists in SQL endpoint)")
                results.append(("clean", "table_exists"))
            else:
                print(
                    f"  → ORPHAN ✅ ({schema}.{table} NOT found in SQL endpoint)")
                results.append(("orphan", "table_deleted"))

            continue

        # Fallback
        print("  → SKIP (unhandled asset type)")
        results.append(("skip", f"unhandled_{atype}"))

    result_df = pd.DataFrame(
        results,
        columns=["status", "reason"],
        index=df.index
    )
    result_df["is_orphan"] = result_df["status"] == "orphan"

    return pd.concat([df, result_df], axis=1)


# ──────────────────────────────────────────────────────────────────
# DELETE
# ──────────────────────────────────────────────────────────────────

def delete_orphans(
    orphan_df: pd.DataFrame,
    config: Config,
    purview_client: PurviewClient,
    dry_run: bool = True,
):
    guids = orphan_df["id"].dropna().tolist()
    token = purview_client.credential.get_token(
        "https://purview.azure.net/.default").token
    hdrs = {"Authorization": f"Bearer {token}",
            "Content-Type": "application/json"}
    success, failed = [], []

    print(
        f"\n=== {'[DRY RUN] ' if dry_run else ''}Deleting {len(guids)} orphaned assets ===")

    for idx, guid in enumerate(guids, start=1):
        if idx % 4000 == 0:
            token = purview_client.credential.get_token(
                "https://purview.azure.net/.default").token
            hdrs["Authorization"] = f"Bearer {token}"
            print("  🔄 Token refreshed.")

        if dry_run:
            print(f"  [DRY RUN] Would delete: {guid}")
            success.append(guid)
            continue

        url = f"{config.purview_endpoint}/datamap/api/atlas/v2/entity/guid/{guid}"
        resp = requests.delete(url, headers=hdrs)

        if resp.status_code == 200:
            print(f"  ✅ Deleted: {guid}")
            success.append(guid)
        else:
            print(f"  ❌ Failed {guid}: {resp.status_code} — {resp.text}")
            failed.append(guid)

    print(f"\n  ✅ Success: {len(success)} | ❌ Failed: {len(failed)}")
    return success, failed


# ──────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("🚀 Purview Fabric Orphan Detector")
    print(f"   Collection : {TARGET_COLLECTION_ID}")
    print(f"   Dry run    : {DRY_RUN}")

    config = Config()
    purview_client = PurviewClient(config)
    fabric_client = FabricClient(config)
    sql_client = SQLClient(config)

    # 1. Fetch all assets for the collection
    df = purview_client.get_assets(TARGET_COLLECTION_ID)
    if df.empty:
        print("No assets found. Exiting.")
        return

    # 2. Detect orphans
    df = detect_orphans(df, fabric_client, sql_client)

    # 3. Save reports
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    audit_csv = os.path.join(
        OUTPUT_DIR, f"audit_{TARGET_COLLECTION_ID}_{ts}.csv")
    orphan_csv = os.path.join(
        OUTPUT_DIR, f"orphans_{TARGET_COLLECTION_ID}_{ts}.csv")

    df.to_csv(audit_csv, index=False)

    orphans = df[df["is_orphan"] == True]

    print(f"\n{'='*50}")
    print(f"  Total assets   : {len(df)}")
    print(f"  Orphans found  : {len(orphans)}")
    print(f"  Skipped        : {(df['status'] == 'skip').sum()}")
    print(f"  Clean          : {(df['status'] == 'clean').sum()}")
    print(f"{'='*50}")

    if not orphans.empty:
        show = [c for c in ["name", "qualifiedName",
                            "status", "reason"] if c in orphans.columns]
        print("\nOrphaned assets:")
        print(orphans[show].to_string(index=False))
        orphans.to_csv(orphan_csv, index=False)
        print(f"\n📄 Orphan report : {orphan_csv}")

    print(f"📄 Full audit    : {audit_csv}")

    # 4. Delete
    if orphans.empty:
        print("\n✅ Nothing to delete.")
        return

    if not DRY_RUN:
        confirm = input(
            f"\n⚠️  Delete {len(orphans)} assets from Purview? Type 'yes': ")
        if confirm.strip().lower() != "yes":
            print("Aborted.")
            return

    delete_orphans(orphans, config, purview_client, dry_run=DRY_RUN)


if __name__ == "__main__":
    main()

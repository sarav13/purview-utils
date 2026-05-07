"""
purview_labeler.py
──────────────────
Recursively traverses Microsoft Purview collections starting from a root
collection, builds hierarchical labels from collection paths, and applies
those labels to every asset found - without overwriting existing labels.
"""

from __future__ import annotations

import logging
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Generator, Optional

import requests
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv
import os

# Logging setup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("purview_labeler.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("purview_labeler")


# Configuration

load_dotenv()


@dataclass(frozen=True)
class Config:
    tenant_id: str
    client_id: str
    client_secret: str
    purview_account: str
    root_collection_id: str
    search_page_size: int = 100   # assets per search page
    request_timeout: int = 30     # seconds
    retry_attempts: int = 3
    retry_backoff: float = 2.0    # exponential back-off base (seconds)
    # When True, only label assets that live in child (non-parent) collections.
    # Collections that have sub-collections are traversed for structure but their
    # own assets are skipped.  Root collection assets are always skipped in this mode.
    skip_parent_collections: bool = False
    # When True, the root collection's name is included as the first segment
    # of every generated label.  Default is False (root excluded) which keeps
    # labels short and environment-agnostic.
    include_root_in_label: bool = False

    @classmethod
    def from_env(cls) -> "Config":
        required = [
            "TENANT_ID", "CLIENT_ID", "CLIENT_SECRET",
            "PURVIEW_ACCOUNT", "ROOT_COLLECTION_ID",
        ]
        missing = [k for k in required if not os.getenv(k)]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}"
            )
        return cls(
            tenant_id=os.environ["TENANT_ID"],
            client_id=os.environ["CLIENT_ID"],
            client_secret=os.environ["CLIENT_SECRET"],
            purview_account=os.environ["PURVIEW_ACCOUNT"],
            root_collection_id=os.environ["ROOT_COLLECTION_ID"],
            search_page_size=int(os.getenv("SEARCH_PAGE_SIZE", "100")),
            request_timeout=int(os.getenv("REQUEST_TIMEOUT", "30")),
            retry_attempts=int(os.getenv("RETRY_ATTEMPTS", "3")),
            retry_backoff=float(os.getenv("RETRY_BACKOFF", "2.0")),
            skip_parent_collections=os.getenv(
                "SKIP_PARENT_COLLECTIONS", "false"
            ).lower() in ("1", "true", "yes"),
            include_root_in_label=os.getenv(
                "INCLUDE_ROOT_IN_LABEL", "false"
            ).lower() in ("1", "true", "yes"),
        )


# Data models

@dataclass
class CollectionNode:
    name: str           # friendly / display name
    qualified_name: str  # unique system name used in API calls
    parent_name: Optional[str] = None
    children: list["CollectionNode"] = field(default_factory=list)
    path_parts: list[str] = field(
        default_factory=list)  # display-name breadcrumb

    def path_label(self, include_root: bool = False) -> str:
        """
        Converts the breadcrumb path to a normalized label.

        include_root=False (default):
            ['DaaS Dev', 'Client Silver', 'Finance'] → 'client-silver-finance'
        include_root=True:
            ['DaaS Dev', 'Client Silver', 'Finance'] → 'daas-dev-client-silver-finance'

        If this node IS the root (only one path part), the label always uses
        that single segment regardless of the flag.
        """
        parts = self.path_parts if (include_root or len(
            self.path_parts) == 1) else self.path_parts[1:]
        joined = "-".join(parts)
        normalized = re.sub(
            r"[^a-z0-9\-]", "", joined.lower().replace(" ", "-").replace("_", "-"))
        return re.sub(r"-{2,}", "-", normalized).strip("-")

    @property
    def is_leaf(self) -> bool:
        """True when this collection has no child collections."""
        return len(self.children) == 0

    @property
    def hierarchy_display(self) -> str:
        """Human-readable path like 'DaaS Dev → Client Silver → Finance'."""
        return " → ".join(self.path_parts)


@dataclass
class LabelingStats:
    collections_visited: int = 0
    assets_fetched: int = 0
    assets_updated: int = 0
    assets_skipped: int = 0
    assets_failed: int = 0

    def summary(self) -> str:
        return (
            f"Collections visited : {self.collections_visited}\n"
            f"Assets fetched      : {self.assets_fetched}\n"
            f"Assets updated      : {self.assets_updated}\n"
            f"Assets skipped      : {self.assets_skipped} (label already present)\n"
            f"Assets failed       : {self.assets_failed}"
        )


# Authentication

class PurviewAuth:
    """Wraps Azure AD Service Principal token acquisition."""

    PURVIEW_SCOPE = "https://purview.azure.net/.default"

    def __init__(self, cfg: Config) -> None:
        self._credential = ClientSecretCredential(
            tenant_id=cfg.tenant_id,
            client_id=cfg.client_id,
            client_secret=cfg.client_secret,
        )
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0

    def get_token(self) -> str:
        """Returns a cached token or fetches a new one when close to expiry."""
        now = time.monotonic()
        if self._token and now < self._token_expiry - 60:
            return self._token
        token_obj = self._credential.get_token(self.PURVIEW_SCOPE)
        self._token = token_obj.token
        self._token_expiry = token_obj.expires_on
        logger.debug("Azure AD token refreshed.")
        return self._token

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.get_token()}",
            "Content-Type": "application/json",
        }


# Purview API client

class PurviewClient:
    """
    Low-level HTTP client for Purview REST APIs with retry logic.
    """

    COLLECTION_API_VERSION = "2019-11-01-preview"
    CATALOG_API_VERSION = "2022-08-01-preview"

    def __init__(self, cfg: Config, auth: PurviewAuth) -> None:
        self.cfg = cfg
        self.auth = auth
        self._account_base = (
            f"https://{cfg.purview_account}.purview.azure.com"
        )
        self._catalog_base = (
            f"https://{cfg.purview_account}.purview.azure.com"
        )
        self._session = requests.Session()

    # ── internal helpers ─────────────────────────────────────────────────────

    def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> requests.Response:
        """Executes an HTTP request with exponential back-off retry."""
        kwargs.setdefault("timeout", self.cfg.request_timeout)
        last_exc: Exception = RuntimeError("No attempts made")

        for attempt in range(1, self.cfg.retry_attempts + 1):
            try:
                kwargs["headers"] = self.auth.headers
                resp = self._session.request(method, url, **kwargs)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    logger.warning("Rate-limited. Sleeping %ds …", retry_after)
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                return resp
            except requests.HTTPError as exc:
                last_exc = exc
                if exc.response is not None and exc.response.status_code < 500:
                    raise  # client errors are not retried
                logger.warning("HTTP error on attempt %d/%d: %s",
                               attempt, self.cfg.retry_attempts, exc)
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning("Request error on attempt %d/%d: %s",
                               attempt, self.cfg.retry_attempts, exc)

            sleep_time = self.cfg.retry_backoff ** attempt
            logger.info("Retrying in %.1fs …", sleep_time)
            time.sleep(sleep_time)

        raise last_exc

    # ── Collection APIs ──────────────────────────────────────────────────────

    def get_collection(self, collection_name: str) -> dict:
        """Fetch a single collection by its system name."""
        url = (
            f"{self._account_base}/collections/{collection_name}"
            f"?api-version={self.COLLECTION_API_VERSION}"
        )
        return self._request("GET", url).json()

    def list_child_collections(self, parent_name: str) -> list[dict]:
        """
        Returns all direct children of *parent_name*.
        Handles nextLink-based pagination from the collections API.
        """
        url = (
            f"{self._account_base}/collections"
            f"?api-version={self.COLLECTION_API_VERSION}"
        )
        children: list[dict] = []
        while url:
            data = self._request("GET", url).json()
            for col in data.get("value", []):
                parent_ref = col.get("parentCollection", {}).get(
                    "referenceName", "")
                if parent_ref == parent_name:
                    children.append(col)
            url = data.get("nextLink")
        return children

    # ── Search / Asset APIs ──────────────────────────────────────────────────

    def search_assets_in_collection(
        self, collection_name: str
    ) -> Generator[dict, None, None]:
        """
        Yields every asset in *collection_name* using offset-based pagination.
        Uses POST /catalog/api/search/query with a collectionId filter.
        """
        url = (
            f"{self._catalog_base}/catalog/api/search/query"
            f"?api-version={self.CATALOG_API_VERSION}"
        )
        offset = 0
        limit = self.cfg.search_page_size
        fetched_on_page = limit  # bootstrap loop

        while fetched_on_page == limit:
            payload = {
                "keywords": None,
                "limit": limit,
                "offset": offset,
                "filter": {
                    "collectionId": collection_name,
                },
            }
            data = self._request("POST", url, json=payload).json()
            results = data.get("value", [])
            fetched_on_page = len(results)

            for asset in results:
                yield asset

            offset += fetched_on_page

    # ── Label APIs ───────────────────────────────────────────────────────────

    def get_entity_labels(self, guid: str) -> set[str]:
        """Retrieve the current labels on an entity."""
        url = (
            f"{self._catalog_base}/catalog/api/atlas/v2/entity/guid/{guid}"
            f"?api-version={self.CATALOG_API_VERSION}"
        )
        try:
            data = self._request("GET", url).json()
            labels = data.get("entity", {}).get("labels", [])
            return set(labels)
        except Exception as exc:
            logger.warning("Could not fetch labels for %s: %s", guid, exc)
            return set()

    def set_entity_labels(self, guid: str, labels: set[str]) -> None:
        """
        Replaces the full label set on *guid*.
        PUT /catalog/api/atlas/v2/entity/guid/{guid}/labels
        Body: ["label1", "label2", …]
        """
        url = (
            f"{self._catalog_base}/catalog/api/atlas/v2/entity/guid/{guid}/labels"
            f"?api-version={self.CATALOG_API_VERSION}"
        )
        self._request("PUT", url, json=list(labels))


# Collection tree builder

class CollectionTreeBuilder:
    """
    Recursively fetches all child collections and assembles a tree.
    """

    def __init__(self, client: PurviewClient) -> None:
        self.client = client

    def build(self, root_name: str) -> CollectionNode:
        """
        Fetches *root_name* from Purview, then recursively adds children.
        Returns the fully-populated root CollectionNode.
        """
        raw = self.client.get_collection(root_name)
        root = CollectionNode(
            name=raw.get("friendlyName", root_name),
            qualified_name=root_name,
            path_parts=[raw.get("friendlyName", root_name)],
        )
        logger.info("Root collection: '%s' (%s)", root.name, root_name)
        self._attach_children(root)
        return root

    def _attach_children(self, parent: CollectionNode) -> None:
        raw_children = self.client.list_child_collections(
            parent.qualified_name)
        logger.debug(
            "Collection '%s' has %d child(ren).", parent.qualified_name, len(
                raw_children)
        )
        for raw in raw_children:
            child_qname = raw.get("name", "")
            child_fname = raw.get("friendlyName", child_qname)
            child = CollectionNode(
                name=child_fname,
                qualified_name=child_qname,
                parent_name=parent.qualified_name,
                path_parts=parent.path_parts + [child_fname],
            )
            parent.children.append(child)
            self._attach_children(child)   # recurse


def walk_tree(node: CollectionNode) -> Generator[CollectionNode, None, None]:
    """DFS generator yielding every node in the tree."""
    yield node
    for child in node.children:
        yield from walk_tree(child)


# Label generator

class LabelGenerator:
    """
    Derives all labels for a collection node from its hierarchy path.

    Given the tree:  fabric → daas-test → finance-bronze
    And ROOT = fabric, the effective path parts (root excluded) are:
        ['daas-test', 'finance-bronze']

    This produces four labels:
        daas-test              ← environment  (level-1 segment, whole)
        finance                ← domain       (first word of leaf collection name)
        bronze                 ← layer        (last word of leaf collection name)
        daas-test-finance-bronze ← super tag  (full combo, root excluded)

    Assumptions about collection naming convention:
        • Level immediately below root  = environment  e.g. "daas-test", "daas-prod"
        • Leaf collection name          = "<domain>-<layer>"  e.g. "finance-bronze"
          The LAST hyphen-delimited token is treated as the medallion layer.
          Everything before it is the domain (handles multi-word domains too).

    include_root controls whether the root segment is prepended to the super tag.
    """

    KNOWN_LAYERS = {"bronze", "silver", "gold"}

    def __init__(self, include_root: bool = False) -> None:
        self.include_root = include_root

    @staticmethod
    def _normalize(text: str) -> str:
        """Lowercase, spaces/underscores → hyphens, strip non-alnum-hyphen, collapse hyphens."""
        s = text.lower().replace(" ", "-").replace("_", "-")
        s = re.sub(r"[^a-z0-9\-]", "", s)
        return re.sub(r"-{2,}", "-", s).strip("-")

    def generate(self, node: CollectionNode) -> set[str]:
        """
        Derives all four labels from the collection hierarchy path.

        Expected path structure (positions are fixed):
            Index 0  — root          e.g. "fabric"          → excluded from all labels
            Index 1  — environment   e.g. "daas-dev"        → environment label
            Index 2+ — collection    e.g. "client-bronze"   → domain + layer labels
                                                              (last token = layer,
                                                               everything before = domain)

        Produced labels for fabric → daas-dev → client-bronze:
            "daas-dev"              ← environment
            "client"                ← domain
            "bronze"                ← layer
            "daas-dev-client-bronze" ← super tag (env + full collection name)

        Falls back gracefully for shallower paths (e.g. env-only nodes):
            emits what it can and logs a warning for missing segments.
        """
        all_parts = [self._normalize(p) for p in node.path_parts]
        # Strip root (index 0); remaining parts are [env, collection, ...]
        child_parts = all_parts[1:] if len(all_parts) > 1 else all_parts

        labels: set[str] = set()

        # ── 1. Environment label (index 1 of full path = index 0 of child_parts) ──
        env_label = child_parts[0] if len(child_parts) >= 1 else None
        if env_label:
            labels.add(env_label)

        # ── 2. Domain + Layer (index 2+ = index 1+ of child_parts) ──────────
        # Collection name is the last segment; split on "-" to get domain + layer.
        # If child_parts has only one element, env == collection (shallow node) —
        # still attempt domain/layer split but skip the super tag duplication.
        collection_segment = child_parts[-1] if child_parts else ""
        tokens = collection_segment.split("-") if collection_segment else []

        if len(tokens) >= 2:
            layer_label = tokens[-1]
            domain_label = "-".join(tokens[:-1])
            labels.add(layer_label)
            labels.add(domain_label)
        elif len(tokens) == 1 and tokens[0]:
            logger.warning(
                "Collection '%s': name '%s' has no hyphen — "
                "cannot split domain and layer. Only env and super-tag applied.",
                node.qualified_name, collection_segment,
            )
        else:
            logger.warning(
                "Collection '%s' has an empty name segment — skipping domain/layer.",
                node.qualified_name,
            )

        # ── 3. Super tag: env + full collection name joined ──────────────────
        # Built from child_parts (root always excluded from super tag content)
        # but prepend the root segment too when include_root=True.
        super_parts = all_parts if self.include_root else child_parts
        super_tag = re.sub(r"-{2,}", "-", "-".join(super_parts)).strip("-")
        if super_tag and super_tag not in labels:   # avoid dup when path is 1-level
            labels.add(super_tag)

        logger.debug(
            "  Labels derived for '%s': %s", node.hierarchy_display, sorted(
                labels)
        )
        return labels


# Label application engine

class LabelApplicator:
    """
    For each collection node, fetches assets and applies the hierarchical label.
    Existing labels are preserved (merge strategy).
    """

    def __init__(self, client: PurviewClient, stats: LabelingStats, cfg: Config) -> None:
        self.client = client
        self.stats = stats
        self.generator = LabelGenerator(include_root=cfg.include_root_in_label)

    def process_collection(self, node: CollectionNode) -> None:
        labels_to_add: set[str] = self.generator.generate(node)

        self.stats.collections_visited += 1

        logger.info(
            "──────────────────────────────────────────────────────\n"
            "  Collection : %s\n"
            "  Path       : %s\n"
            "  Labels     : %s",
            node.qualified_name,
            node.hierarchy_display,
            sorted(labels_to_add),
        )

        asset_count = 0
        for asset in self.client.search_assets_in_collection(node.qualified_name):
            asset_count += 1
            self.stats.assets_fetched += 1
            guid = asset.get("id") or asset.get("guid")
            asset_name = asset.get("name", "<unnamed>")
            asset_type = asset.get("entityType", "unknown")

            if not guid:
                logger.warning(
                    "  ⚠  Asset '%s' has no guid — skipped.", asset_name)
                self.stats.assets_skipped += 1
                continue

            self._apply_labels(guid, asset_name, asset_type, labels_to_add)

        logger.info(
            "  Processed %d asset(s) in '%s'.", asset_count, node.qualified_name
        )

    def _apply_labels(
        self, guid: str, name: str, entity_type: str, new_labels: set[str]
    ) -> None:
        try:
            existing = self.client.get_entity_labels(guid)

            to_add = new_labels - existing          # only truly new labels
            if not to_add:
                logger.debug(
                    "    ↩  [%s] '%s' already has all labels %s — skipped.",
                    entity_type, name, sorted(new_labels),
                )
                self.stats.assets_skipped += 1
                return

            merged = existing | new_labels
            self.client.set_entity_labels(guid, merged)

            logger.info(
                "    ✓  [%s] '%s' → added %s | full set: %s",
                entity_type, name, sorted(to_add), sorted(merged),
            )
            self.stats.assets_updated += 1

        except Exception as exc:
            logger.error(
                "    ✗  [%s] '%s' (guid=%s) — label update failed: %s",
                entity_type, name, guid, exc,
            )
            self.stats.assets_failed += 1


# Orchestrator

class PurviewLabelingOrchestrator:
    """
    Ties all components together and drives the end-to-end workflow:
      1. Build collection tree from root
      2. Walk each node
      3. Generate label from hierarchy path
      4. Fetch assets in collection
      5. Merge + apply labels
    """

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        auth = PurviewAuth(cfg)
        self.client = PurviewClient(cfg, auth)
        self.stats = LabelingStats()

    def run(self) -> None:
        logger.info("════════════════════════════════════════════════════════")
        logger.info("  Microsoft Purview Hierarchical Label Applicator")
        logger.info("  Account         : %s", self.cfg.purview_account)
        logger.info("  Root collection : %s", self.cfg.root_collection_id)
        logger.info("════════════════════════════════════════════════════════")

        # Step 1 — build collection tree
        builder = CollectionTreeBuilder(self.client)
        root = builder.build(self.cfg.root_collection_id)
        self._log_tree(root, indent=0)

        # Step 2 — walk tree and apply labels
        applicator = LabelApplicator(self.client, self.stats, self.cfg)
        skip_parents = self.cfg.skip_parent_collections
        if skip_parents:
            logger.info(
                "  Mode: SKIP_PARENT_COLLECTIONS=true — "
                "only leaf (childless) collections will have their assets labelled."
            )
        for node in walk_tree(root):
            if skip_parents and not node.is_leaf:
                logger.info(
                    "  ⏭  Skipping parent collection '%s' (%s child(ren)).",
                    node.hierarchy_display,
                    len(node.children),
                )
                continue
            applicator.process_collection(node)

        # Step 3 — final summary
        logger.info("════════════════════════════════════════════════════════")
        logger.info("  Run complete. Summary:\n%s", self.stats.summary())
        logger.info("════════════════════════════════════════════════════════")

    @staticmethod
    def _log_tree(node: CollectionNode, indent: int) -> None:
        prefix = "    " * indent + ("└── " if indent else "")
        logger.info("%s%s  [label: %s]", prefix,
                    node.hierarchy_display, node.path_label())
        for child in node.children:
            PurviewLabelingOrchestrator._log_tree(child, indent + 1)


if __name__ == "__main__":
    cfg = Config.from_env()
    PurviewLabelingOrchestrator(cfg).run()

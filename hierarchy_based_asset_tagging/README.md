# Purview Hierarchical Tagging

Recursively traverses Microsoft Purview collections from a configurable root,
generates normalized hierarchical labels from collection paths, and applies
them to every asset - preserving all existing labels.

---

## Architecture

```
purview_labeler/
├── purview_labeler.py        ← single-file solution (all modules inside)
├── test_purview_labeler.py   ← pytest unit tests (no Purview connection needed)
├── requirements.txt
├── .env.template             ← copy to .env and fill in values
└── README.md
```

### Internal module map

| Class / Component          | Responsibility                                          |
|----------------------------|---------------------------------------------------------|
| `Config`                   | Loads & validates all settings from `.env`             |
| `PurviewAuth`              | Service Principal → Azure AD token (auto-refresh)      |
| `PurviewClient`            | All HTTP calls to Purview REST APIs + retry logic      |
| `CollectionNode`           | Tree node + label / display-path generation            |
| `CollectionTreeBuilder`    | Recursively builds the collection tree                 |
| `walk_tree()`              | DFS generator over the entire tree                     |
| `LabelApplicator`          | Fetch assets → merge labels → apply via PUT            |
| `LabelingStats`            | Counters for collections, assets, updates, failures    |
| `PurviewLabelingOrchestrator` | Wires everything together, drives the run          |

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.template .env
# Edit .env with your values
```

| Variable            | Description                                                  |
|---------------------|--------------------------------------------------------------|
| `TENANT_ID`         | Azure Active Directory tenant ID                            |
| `CLIENT_ID`         | Service Principal application (client) ID                   |
| `CLIENT_SECRET`     | Service Principal client secret                             |
| `PURVIEW_ACCOUNT`   | Purview account name (not the full URL)                     |
| `ROOT_COLLECTION_ID`| System id of the collection (not the friendly name)         |
| `SEARCH_PAGE_SIZE`  | Assets per page (default `100`, max `1000`)                 |
| `REQUEST_TIMEOUT`   | HTTP timeout in seconds (default `30`)                      |
| `RETRY_ATTEMPTS`    | Retry count on 5xx errors (default `3`)                     |
| `RETRY_BACKOFF`     | Exponential back-off base in seconds (default `2.0`)        |

> **Finding the system collection name**  
> Purview Studio → Data Map → Collections → select your collection →  
> the URL shows `?collectionId=<system-name>` — use that value.

### 3. Grant the Service Principal permissions

In Purview Studio → Management → Role assignments, the SP needs:

- **Data Curator** — to read and update entity labels  
- **Collection Admin** (or **Data Reader**) — to list collections

---

## Running

```bash
python purview_labeler.py
```

Logs are written to both stdout and `purview_labeler.log`.

---

## Example output

```
2024-05-06 10:00:00 | INFO     | purview_labeler | Root collection: 'DaaS Dev' (daasdev)
2024-05-06 10:00:01 | INFO     | purview_labeler | DaaS Dev  [label: daas-dev]
2024-05-06 10:00:01 | INFO     | purview_labeler |     └── DaaS Dev → Client Bronze  [label: daas-dev-client-bronze]
2024-05-06 10:00:01 | INFO     | purview_labeler |     └── DaaS Dev → Client Silver  [label: daas-dev-client-silver]
2024-05-06 10:00:01 | INFO     | purview_labeler |         └── DaaS Dev → Client Silver → Finance  [label: daas-dev-client-silver-finance]

2024-05-06 10:00:03 | INFO     | purview_labeler |   Collection : daasdev-client-bronze
2024-05-06 10:00:03 | INFO     | purview_labeler |   Path       : DaaS Dev → Client Bronze
2024-05-06 10:00:03 | INFO     | purview_labeler |   Label      : daas-dev-client-bronze
2024-05-06 10:00:04 | INFO     | purview_labeler |     ✓  [azure_sql_table] 'sales_raw' → labels: ['daas-dev-client-bronze', 'pii']
2024-05-06 10:00:04 | INFO     | purview_labeler |     ↩  [azure_sql_table] 'orders' already has label 'daas-dev-client-bronze' — skipped.

════════════════════════════════════════════════════════
  Run complete. Summary:
  Collections visited : 4
  Assets fetched      : 312
  Assets updated      : 289
  Assets skipped      : 21 (label already present)
  Assets failed       : 2
════════════════════════════════════════════════════════
```

## Key design decisions

| Decision | Rationale |
|---|---|
| Offset-based pagination only | Purview Search does not support cursor/ID-based pagination for the `/search/query` endpoint |
| No `orderby` in search payload | Purview rejects many `orderby` formats; omitting it is safer |
| Token auto-refresh | `azure-identity` `ClientSecretCredential` handles AAD token expiry transparently |
| Merge before PUT | `GET entity → merge → PUT labels` prevents overwriting business-assigned labels |
| Exponential back-off | Handles transient 5xx and rate-limit (429) responses gracefully |
| `CollectionNode.path_label` is pure | Label derivation has no I/O, making it fully unit-testable without Purview access |

---

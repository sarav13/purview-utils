import pandas as pd


def export_pii_columns(client, collection_id):

    payload = {
        "keywords": None,
        "filter": {
            "and": [
                {"or": [{"assetType": "SQL Server"}]},
                {"or": [{"objectType": "Tables"}]},
                {"or": [{"collectionId": collection_id}]}
            ]
        },
        "limit": 25
    }

    data = client.post(
        "/catalog/api/search/query?api-version=2023-09-01",
        payload
    )

    assets = data.get("value", [])

    records = []

    for asset in assets:

        table_name = asset["name"]
        table_guid = asset["id"]

        entity = client.get(
            f"/catalog/api/atlas/v2/entity/guid/{table_guid}?api-version=2023-09-01"
        )

        columns = entity.get("entity", {}) \
                        .get("relationshipAttributes", {}) \
                        .get("columns", [])

        for col in columns:

            col_guid = col["guid"]

            col_entity = client.get(
                f"/catalog/api/atlas/v2/entity/guid/{col_guid}?api-version=2023-09-01"
            )

            classifications = col_entity.get("entity", {}) \
                                        .get("classifications", [])

            for c in classifications:

                records.append({
                    "table": table_name,
                    "column": col["displayText"],
                    "classification": c["typeName"]
                })

    return pd.DataFrame(records)

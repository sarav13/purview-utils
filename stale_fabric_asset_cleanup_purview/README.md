## Stale Fabric Asset Cleanup in Purview

### Background

Understanding how Fabric assets are represented in Purview is key to identifying stale/orphaned assets.

## Prerequisites:
- Python 3.8 or higher
- Access to a Microsoft Purview account
- Access to Microsoft Fabric workspace (Lakehouse + SQL Endpoint)
- Azure AD App Registration (Service Principal) with required permissions

Required Python packages (install via `pip install -r requirements.txt`):

   - azure-purview-datamap
   - azure-identity
   - azure-core
   - pandas
   - python-dotenv
   - requests
   - msal
   - pyodbc

---

## Usage
1. Set up your environment variables -provided an sample .env file
2. Execute the script
   "python cleaup_fabric_assets.py"


#### Fabric → Purview Structure

Purview Collection
   - Workspace
        - Lakehouse1
            - SQL Endpoint        (1:1 with each Lakehouse)
              - dbo.table1      ← registered as individual Purview assets
              - dbo.table2
              - analytics.view1
        - Lakehouse2
            - SQL Endpoint
              - dbo.orders
              - dbo.customers

---

### Problem

Purview scans **tables and views via the Lakehouse SQL Endpoint**, not directly from underlying storage.

- When a table is deleted in Fabric:
  - It is removed from the **SQL Endpoint**
  - But **still remains in Purview** as an asset

This leads to **stale/orphaned assets** in the Purview catalog.

---

### Key Insight

> The **SQL Endpoint is the source of truth** for what actually exists.

---

### Solution Approach

The cleanup tool validates assets by cross-referencing **three systems**:

1. **Purview Catalog**  
   - What Purview believes exists

2. **Fabric REST API**  
   - Confirms if the Lakehouse itself still exists

3. **Fabric SQL Endpoint**  
   - Verifies if the table/view actually exists

---

### Logic Summary

An asset is considered **stale** if:

- It exists in **Purview**
- But does **NOT exist in SQL Endpoint**
- And optionally, its parent Lakehouse may or may not exist

---

### Outcome

- Identifies orphaned/stale assets
- Enables safe cleanup
- Keeps Purview catalog accurate and trustworthy

---

## Future Improvements 

- Soft-delete validation before permanent removal  
- Logging + audit trail for deleted assets  
- Dashboard/report for stale asset tracking  
- Scheduling via pipeline or automation  

---

## Notes

- Designed during initial exploration of Purview + Fabric integration  
- Focused on improving catalog accuracy and governance hygiene  

---

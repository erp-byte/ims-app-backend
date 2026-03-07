# RTV (Return to Vendor) Module — Frontend Integration Guide

> **Base URL prefix:** `/rtv`
>
> **Company parameter:** `"CFPL"` or `"CDPL"` (string literal, case-sensitive)

---

## Table of Contents

1. [Data Types & Field Reference](#1-data-types--field-reference)
2. [Endpoints](#2-endpoints)
   - [Create RTV](#21-create-rtv)
   - [List RTVs](#22-list-rtvs)
   - [Get RTV Detail](#23-get-rtv-detail)
   - [Update RTV Header](#24-update-rtv-header)
   - [Delete RTV](#25-delete-rtv)
   - [Update RTV Lines](#26-update-rtv-lines)
   - [Upsert RTV Box (Print)](#27-upsert-rtv-box-print)
   - [Approve RTV](#28-approve-rtv)
   - [Log Box Edits](#29-log-box-edits)
   - [Export to Excel](#210-export-to-excel)
3. [RTV Lifecycle & Status Flow](#3-rtv-lifecycle--status-flow)
4. [Box ID Generation Logic](#4-box-id-generation-logic)
5. [Database Tables](#5-database-tables)
6. [Important Notes](#6-important-notes)

---

## 1. Data Types & Field Reference

### Company (Path Parameter)

| Value  | Description               |
|--------|---------------------------|
| `CFPL` | Tables prefixed `cfpl_`   |
| `CDPL` | Tables prefixed `cdpl_`   |

### Custom Decimal Types

| Type          | Description                              |
|---------------|------------------------------------------|
| `Decimal18_2` | Decimal, max 18 digits, 2 decimal places |
| `Decimal18_3` | Decimal, max 18 digits, 3 decimal places |

### RTV Header Fields

| Field              | Type              | Required on Create | Nullable | Notes                                |
|--------------------|-------------------|--------------------|----------|--------------------------------------|
| `id`               | `int`             | Auto-generated     | No       | DB primary key (used as path param)  |
| `rtv_id`           | `string`          | Auto-generated     | No       | Format: `RTV-YYYYMMDDHHmmSS`        |
| `rtv_date`         | `datetime`        | Auto (NOW())       | Yes      | Set at creation time                 |
| `factory_unit`     | `string`          | Yes                | No       |                                      |
| `customer`         | `string`          | Yes                | No       |                                      |
| `invoice_number`   | `string`          | No                 | Yes      |                                      |
| `challan_no`       | `string`          | No                 | Yes      |                                      |
| `dn_no`            | `string`          | No                 | Yes      |                                      |
| `conversion`       | `string`          | No                 | Yes      | Default `"0"`. Stored as float in DB |
| `sales_poc`        | `string`          | No                 | Yes      |                                      |
| `remark`           | `string`          | No                 | Yes      |                                      |
| `status`           | `string`          | Auto (`"Pending"`) | No       | `"Pending"` / `"Approved"`           |
| `created_by`       | `string`          | Query param        | Yes      | Email of creator                     |
| `created_ts`       | `datetime`        | Auto (NOW())       | Yes      |                                      |
| `updated_at`       | `datetime`        | Auto               | Yes      | Set on every update                  |

### RTV Line Fields

| Field               | Type       | Required on Create | Nullable | Notes                                          |
|---------------------|------------|--------------------|----------|-------------------------------------------------|
| `id`                | `int`      | Auto-generated     | No       | DB primary key                                  |
| `header_id`         | `int`      | Auto (FK)          | No       | References header `id`                          |
| `material_type`     | `string`   | Yes                | No       | **Auto-uppercased** by validator                |
| `item_category`     | `string`   | Yes                | No       |                                                 |
| `sub_category`      | `string`   | Yes                | No       |                                                 |
| `item_description`  | `string`   | Yes                | No       | Used as lookup key for box ↔ line linking        |
| `uom`               | `string`   | Yes                | No       | **Auto-uppercased** by validator                |
| `qty`               | `string`   | No                 | No       | Default `"0"`. Converted to `int` in DB         |
| `rate`              | `string`   | No                 | No       | Default `"0"`. Converted to `float` in DB       |
| `value`             | `string`   | No                 | No       | Default `"0"`. If `"0"`, computed as `qty * rate`|
| `net_weight`        | `string`   | No                 | Yes      | Default `"0"`. Converted to `float` in DB       |
| `created_at`        | `datetime` | Auto               | Yes      |                                                 |
| `updated_at`        | `datetime` | Auto               | Yes      |                                                 |

### RTV Box Fields

| Field                 | Type          | Required on Upsert | Nullable | Notes                                          |
|-----------------------|---------------|--------------------|----------|-------------------------------------------------|
| `id`                  | `int`         | Auto-generated     | No       | DB primary key                                  |
| `header_id`           | `int`         | Auto (FK)          | No       | References header `id`                          |
| `rtv_line_id`         | `int`         | Auto-resolved      | Yes      | Resolved by matching `article_description` to line's `item_description` |
| `box_number`          | `int`         | Yes (>= 1)         | No       | Sequential per article per RTV                  |
| `box_id`              | `string`      | Auto-generated     | Yes      | `NULL` until Print; format: `{8-digit-epoch}-{box_number}` |
| `article_description` | `string`      | Yes                | No       | Must match a line's `item_description`          |
| `lot_number`          | `string`      | No                 | Yes      |                                                 |
| `net_weight`          | `Decimal18_3` | No                 | No       | Default `"0"`                                   |
| `gross_weight`        | `Decimal18_3` | No                 | No       | Default `"0"`                                   |
| `count`               | `int`         | No                 | Yes      |                                                 |
| `created_at`          | `datetime`    | Auto               | Yes      |                                                 |
| `updated_at`          | `datetime`    | Auto               | Yes      |                                                 |

---

## 2. Endpoints

### 2.1 Create RTV

Create a new RTV with header and line items.

| Property | Value |
|----------|-------|
| **Method** | `POST` |
| **URL** | `/rtv/{company}` |
| **Status** | `201 Created` |

**Path Parameters:**

| Param     | Type     | Description       |
|-----------|----------|-------------------|
| `company` | `string` | `"CFPL"` or `"CDPL"` |

**Query Parameters:**

| Param        | Type     | Default              | Description           |
|--------------|----------|----------------------|-----------------------|
| `created_by` | `string` | `"user@example.com"` | Email of the creator  |

**Request Body** (`RTVCreate`):

```json
{
  "company": "CFPL",
  "header": {
    "factory_unit": "Unit-A",
    "customer": "Customer Name",
    "invoice_number": "INV-001",
    "challan_no": "CH-001",
    "dn_no": "DN-001",
    "conversion": "1.5",
    "sales_poc": "John Doe",
    "remark": "Sample remark"
  },
  "lines": [
    {
      "material_type": "RM",
      "item_category": "Category A",
      "sub_category": "Sub A",
      "item_description": "Steel Rod 10mm",
      "uom": "KG",
      "qty": "100",
      "rate": "50",
      "value": "5000",
      "net_weight": "100"
    }
  ]
}
```

> **Note:** `lines` must have at least 1 item. `material_type` and `uom` are auto-uppercased.

**Response** (`RTVWithDetails`):

```json
{
  "id": 1,
  "rtv_id": "RTV-20260302143022",
  "rtv_date": "2026-03-02T14:30:22",
  "factory_unit": "Unit-A",
  "customer": "Customer Name",
  "invoice_number": "INV-001",
  "challan_no": "CH-001",
  "dn_no": "DN-001",
  "conversion": "1.5",
  "sales_poc": "John Doe",
  "remark": "Sample remark",
  "status": "Pending",
  "created_by": "user@example.com",
  "created_ts": "2026-03-02T14:30:22",
  "updated_at": null,
  "lines": [
    {
      "id": 1,
      "header_id": 1,
      "material_type": "RM",
      "item_category": "Category A",
      "sub_category": "Sub A",
      "item_description": "Steel Rod 10mm",
      "uom": "KG",
      "qty": "100",
      "rate": "50.0",
      "value": "5000.0",
      "net_weight": "100.0",
      "created_at": "2026-03-02T14:30:22",
      "updated_at": null
    }
  ],
  "boxes": []
}
```

---

### 2.2 List RTVs

Paginated list with filters and sorting.

| Property | Value |
|----------|-------|
| **Method** | `GET` |
| **URL** | `/rtv/{company}` |

**Path Parameters:**

| Param     | Type     | Description       |
|-----------|----------|-------------------|
| `company` | `string` | `"CFPL"` or `"CDPL"` |

**Query Parameters:**

| Param          | Type     | Default        | Constraints     | Description                          |
|----------------|----------|----------------|-----------------|--------------------------------------|
| `page`         | `int`    | `1`            | `>= 1`          | Page number                          |
| `per_page`     | `int`    | `10`           | `1–100`         | Records per page                     |
| `status`       | `string` | `null`         |                 | Exact match filter (e.g. `"Pending"`) |
| `factory_unit` | `string` | `null`         |                 | Exact match filter                   |
| `customer`     | `string` | `null`         |                 | Case-insensitive partial match (ILIKE) |
| `from_date`    | `string` | `null`         | `DD-MM-YYYY`    | RTV date >= this date                |
| `to_date`      | `string` | `null`         | `DD-MM-YYYY`    | RTV date <= this date                |
| `sort_by`      | `string` | `"created_ts"` | See allowed list | Sort column                          |
| `sort_order`   | `string` | `"desc"`       | `"asc"` / `"desc"` | Sort direction                   |

**Allowed `sort_by` values:** `rtv_id`, `rtv_date`, `factory_unit`, `customer`, `status`, `created_ts`

**Response** (`RTVListResponse`):

```json
{
  "records": [
    {
      "id": 1,
      "rtv_id": "RTV-20260302143022",
      "rtv_date": "2026-03-02T14:30:22",
      "factory_unit": "Unit-A",
      "customer": "Customer Name",
      "invoice_number": "INV-001",
      "challan_no": "CH-001",
      "dn_no": "DN-001",
      "conversion": "1.5",
      "sales_poc": "John Doe",
      "remark": "Sample remark",
      "status": "Pending",
      "created_by": "user@example.com",
      "created_ts": "2026-03-02T14:30:22",
      "updated_at": null,
      "items_count": 3,
      "boxes_count": 5,
      "total_qty": 250
    }
  ],
  "total": 42,
  "page": 1,
  "per_page": 10,
  "total_pages": 5
}
```

---

### 2.3 Get RTV Detail

Fetch a single RTV with all its lines and boxes.

| Property | Value |
|----------|-------|
| **Method** | `GET` |
| **URL** | `/rtv/{company}/{rtv_id}` |

**Path Parameters:**

| Param     | Type     | Description                     |
|-----------|----------|---------------------------------|
| `company` | `string` | `"CFPL"` or `"CDPL"`           |
| `rtv_id`  | `int`    | Header `id` (DB primary key)    |

**Response** (`RTVWithDetails`): Same structure as [Create RTV response](#21-create-rtv), with populated `lines` and `boxes` arrays.

**Errors:**

| Status | Condition         |
|--------|-------------------|
| `404`  | RTV not found     |

---

### 2.4 Update RTV Header

Update one or more header fields on an existing RTV.

| Property | Value |
|----------|-------|
| **Method** | `PUT` |
| **URL** | `/rtv/{company}/{rtv_id}` |

**Path Parameters:**

| Param     | Type     | Description                     |
|-----------|----------|---------------------------------|
| `company` | `string` | `"CFPL"` or `"CDPL"`           |
| `rtv_id`  | `int`    | Header `id` (DB primary key)    |

**Request Body** (`RTVHeaderUpdate`) — all fields optional, send only what changed:

```json
{
  "factory_unit": "Unit-B",
  "customer": "New Customer",
  "invoice_number": "INV-002",
  "challan_no": "CH-002",
  "dn_no": "DN-002",
  "conversion": "2.0",
  "sales_poc": "Jane Doe",
  "remark": "Updated remark",
  "status": "Pending"
}
```

> **Note:** `conversion` is converted to `float` before storing. At least one field must be provided, otherwise returns `400`.

**Response** (`RTVHeaderResponse`):

```json
{
  "id": 1,
  "rtv_id": "RTV-20260302143022",
  "rtv_date": "2026-03-02T14:30:22",
  "factory_unit": "Unit-B",
  "customer": "New Customer",
  "invoice_number": "INV-002",
  "challan_no": "CH-002",
  "dn_no": "DN-002",
  "conversion": "2.0",
  "sales_poc": "Jane Doe",
  "remark": "Updated remark",
  "status": "Pending",
  "created_by": "user@example.com",
  "created_ts": "2026-03-02T14:30:22",
  "updated_at": "2026-03-02T15:00:00"
}
```

**Errors:**

| Status | Condition                   |
|--------|-----------------------------|
| `400`  | No fields provided to update |
| `404`  | RTV not found               |

---

### 2.5 Delete RTV

Permanently delete an RTV and **all its lines and boxes**.

| Property | Value |
|----------|-------|
| **Method** | `DELETE` |
| **URL** | `/rtv/{company}/{rtv_id}` |

**Path Parameters:**

| Param     | Type     | Description                     |
|-----------|----------|---------------------------------|
| `company` | `string` | `"CFPL"` or `"CDPL"`           |
| `rtv_id`  | `int`    | Header `id` (DB primary key)    |

**Response** (`RTVDeleteResponse`):

```json
{
  "success": true,
  "message": "RTV RTV-20260302143022 deleted",
  "rtv_id": "RTV-20260302143022"
}
```

**Errors:**

| Status | Condition         |
|--------|-------------------|
| `404`  | RTV not found     |

---

### 2.6 Update RTV Lines

**Replaces all** line items on an existing RTV. Existing lines are deleted and new ones are inserted.

| Property | Value |
|----------|-------|
| **Method** | `PUT` |
| **URL** | `/rtv/{company}/{rtv_id}/lines` |

**Path Parameters:**

| Param     | Type     | Description                     |
|-----------|----------|---------------------------------|
| `company` | `string` | `"CFPL"` or `"CDPL"`           |
| `rtv_id`  | `int`    | Header `id` (DB primary key)    |

**Request Body** (`RTVLinesUpdateRequest`):

```json
{
  "lines": [
    {
      "material_type": "RM",
      "item_category": "Category A",
      "sub_category": "Sub A",
      "item_description": "Steel Rod 10mm",
      "uom": "KG",
      "qty": "150",
      "rate": "55",
      "value": "8250",
      "net_weight": "150"
    },
    {
      "material_type": "FG",
      "item_category": "Category B",
      "sub_category": "Sub B",
      "item_description": "Copper Wire 5mm",
      "uom": "MTR",
      "qty": "200",
      "rate": "30",
      "value": "6000",
      "net_weight": "50"
    }
  ]
}
```

> **Note:** `lines` must have at least 1 item. All previous lines are **deleted** first.

**Response** (`RTVLinesUpdateResponse`):

```json
{
  "status": "updated",
  "rtv_id": "RTV-20260302143022",
  "lines_count": 2
}
```

**Errors:**

| Status | Condition         |
|--------|-------------------|
| `404`  | RTV not found     |

---

### 2.7 Upsert RTV Box (Print)

Insert or update a single box. **Called when the user clicks the Print button.** This is the endpoint that generates the `box_id`.

| Property | Value |
|----------|-------|
| **Method** | `PUT` |
| **URL** | `/rtv/{company}/{rtv_id}/box` |

**Path Parameters:**

| Param     | Type     | Description                     |
|-----------|----------|---------------------------------|
| `company` | `string` | `"CFPL"` or `"CDPL"`           |
| `rtv_id`  | `int`    | Header `id` (DB primary key)    |

**Request Body** (`RTVBoxUpsertRequest`):

```json
{
  "article_description": "Steel Rod 10mm",
  "box_number": 1,
  "net_weight": "45.500",
  "gross_weight": "48.200",
  "lot_number": "LOT-2026-001",
  "count": 50
}
```

| Field                 | Type          | Required | Constraints | Notes |
|-----------------------|---------------|----------|-------------|-------|
| `article_description` | `string`      | Yes      |             | Must match a line's `item_description` |
| `box_number`          | `int`         | Yes      | `>= 1`     | Sequential per article |
| `net_weight`          | `Decimal18_3` | No       |             |       |
| `gross_weight`        | `Decimal18_3` | No       |             |       |
| `lot_number`          | `string`      | No       |             |       |
| `count`               | `int`         | No       |             |       |

**Behavior:**

| Scenario | Action |
|----------|--------|
| Box exists **with** `box_id` (already printed) | Updates weight/lot/count fields only; preserves existing `box_id` |
| Box exists **without** `box_id` | Generates new `box_id`, updates all fields |
| Box does not exist | Inserts new box with generated `box_id` |

**Response** (`RTVBoxUpsertResponse`):

```json
{
  "status": "inserted",
  "box_id": "50123456-1",
  "rtv_id": "RTV-20260302143022",
  "article_description": "Steel Rod 10mm",
  "box_number": 1
}
```

> `status` is either `"inserted"` or `"updated"`.

**Errors:**

| Status | Condition         |
|--------|-------------------|
| `404`  | RTV not found     |

---

### 2.8 Approve RTV

Approve an RTV. Optionally update header fields, line fields, and upsert boxes in the same call.

| Property | Value |
|----------|-------|
| **Method** | `PUT` |
| **URL** | `/rtv/{company}/{rtv_id}/approve` |

**Path Parameters:**

| Param     | Type     | Description                     |
|-----------|----------|---------------------------------|
| `company` | `string` | `"CFPL"` or `"CDPL"`           |
| `rtv_id`  | `int`    | Header `id` (DB primary key)    |

**Request Body** (`RTVApprovalRequest`):

```json
{
  "approved_by": "approver@example.com",
  "header": {
    "factory_unit": "Unit-A",
    "customer": "Customer Name",
    "invoice_number": "INV-001",
    "challan_no": "CH-001",
    "dn_no": "DN-001",
    "conversion": "1.5",
    "sales_poc": "John Doe",
    "remark": "Approved with changes"
  },
  "lines": [
    {
      "item_description": "Steel Rod 10mm",
      "qty": "120",
      "rate": "55",
      "value": "6600",
      "net_weight": "120",
      "uom": "KG",
      "material_type": "RM",
      "item_category": "Category A",
      "sub_category": "Sub A"
    }
  ],
  "boxes": [
    {
      "article_description": "Steel Rod 10mm",
      "box_number": 1,
      "net_weight": "45.500",
      "gross_weight": "48.200",
      "lot_number": "LOT-2026-001",
      "count": 50
    }
  ]
}
```

| Field         | Type                          | Required | Notes |
|---------------|-------------------------------|----------|-------|
| `approved_by` | `string`                      | Yes      | Email of approver |
| `header`      | `RTVApprovalHeaderFields`     | No       | Only send fields that changed |
| `lines`       | `RTVApprovalLineFields[]`     | No       | Each entry must include `item_description` as key |
| `boxes`       | `RTVApprovalBoxFields[]`      | No       | Upserted: updates if exists, inserts if new |

**Lines update logic:** Each line is matched by `item_description`. Only the non-null fields in each entry are updated. The line is **not** replaced — it's a partial update.

**Boxes upsert logic:** Each box is matched by `article_description` + `box_number`. If found, it's updated. If not found, a new box is inserted (**without** a `box_id` — the box_id is only generated via the Print endpoint).

**Response** (`RTVApprovalResponse`):

```json
{
  "status": "approved",
  "rtv_id": "RTV-20260302143022",
  "company": "CFPL",
  "approved_by": "approver@example.com",
  "approved_at": "2026-03-02T15:30:00+00:00"
}
```

**Errors:**

| Status | Condition         |
|--------|-------------------|
| `404`  | RTV not found     |

---

### 2.9 Log Box Edits

Log audit entries when a user edits a box that has already been printed (has a `box_id`).

| Property | Value |
|----------|-------|
| **Method** | `POST` |
| **URL** | `/rtv/box-edit-log` |

> **Note:** No `{company}` path param — this endpoint writes to a global `box_edit_logs` table.

**Request Body** (`RTVBoxEditLogRequest`):

```json
{
  "email_id": "editor@example.com",
  "box_id": "50123456-1",
  "rtv_id": "RTV-20260302143022",
  "changes": [
    {
      "field_name": "net_weight",
      "old_value": "45.500",
      "new_value": "46.000"
    },
    {
      "field_name": "gross_weight",
      "old_value": "48.200",
      "new_value": "49.000"
    }
  ]
}
```

| Field                | Type     | Required | Notes |
|----------------------|----------|----------|-------|
| `email_id`           | `string` | Yes      | Email of the person making the edit |
| `box_id`             | `string` | Yes      | The printed box's ID |
| `rtv_id`             | `string` | Yes      | The RTV ID string (e.g. `"RTV-..."`) |
| `changes`            | `array`  | Yes      | List of field changes |
| `changes[].field_name`  | `string` | Yes   | Name of the changed field |
| `changes[].old_value`   | `string` | No    | Previous value |
| `changes[].new_value`   | `string` | No    | New value |

**Response:**

```json
{
  "status": "logged",
  "entries": 2
}
```

**When to call:** After the user edits any field on a box that already has a `box_id`. The frontend should track which fields changed and send them here.

---

### 2.10 Export to Excel

Download filtered RTV records as an `.xlsx` file. Edited box cells are highlighted in red (`#FEE2E2`).

| Property | Value |
|----------|-------|
| **Method** | `GET` |
| **URL** | `/rtv/export` |
| **Response** | Binary file download (`application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`) |

> **Important:** This route is defined **before** `/{company}` to avoid path conflicts.

**Query Parameters:**

| Param          | Type     | Default        | Description                          |
|----------------|----------|----------------|--------------------------------------|
| `company`      | `string` | **Required**   | `"CFPL"` or `"CDPL"`                |
| `status`       | `string` | `null`         | Filter by status                     |
| `customer`     | `string` | `null`         | Case-insensitive partial match       |
| `factory_unit` | `string` | `null`         | Exact match filter                   |
| `from_date`    | `string` | `null`         | `DD-MM-YYYY` format                  |
| `to_date`      | `string` | `null`         | `DD-MM-YYYY` format                  |
| `sort_by`      | `string` | `"created_ts"` | Sort column (same allowed list)      |
| `sort_order`   | `string` | `"desc"`       | `"asc"` / `"desc"`                  |

**Response:** File download with name `rtv_{company}_{YYYYMMDD}.xlsx`

**Excel Columns (in order):**

| # | Column Name      | Source        |
|---|------------------|---------------|
| 1 | RTV ID           | Header        |
| 2 | RTV Date         | Header        |
| 3 | Factory Unit     | Header        |
| 4 | Customer         | Header        |
| 5 | Invoice Number   | Header        |
| 6 | Challan No       | Header        |
| 7 | DN No            | Header        |
| 8 | Conversion       | Header        |
| 9 | Sales POC        | Header        |
| 10 | Remark          | Header        |
| 11 | Status          | Header        |
| 12 | Created By      | Header        |
| 13 | Created At      | Header        |
| 14 | Material Type   | Line          |
| 15 | Item Category   | Line          |
| 16 | Sub Category    | Line          |
| 17 | Item Description| Line          |
| 18 | UOM             | Line          |
| 19 | Qty             | Line          |
| 20 | Rate            | Line          |
| 21 | Value           | Line          |
| 22 | Line Net Weight | Line          |
| 23 | Box ID          | Box           |
| 24 | Box Article     | Box           |
| 25 | Box Number      | Box           |
| 26 | Box Net Weight  | Box           |
| 27 | Box Gross Weight| Box           |
| 28 | Box Lot Number  | Box           |
| 29 | Box Count       | Box           |

**Edit Highlighting:** Cells in columns 26–29 (Box Net Weight, Box Gross Weight, Box Lot Number, Box Count) are highlighted with a red background (`#FEE2E2`) if that box+field combination has an entry in the `box_edit_logs` table.

---

## 3. RTV Lifecycle & Status Flow

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Create   │────>│ Pending  │────>│ Approved │
│  (POST)   │     │          │     │  (PUT)   │
└──────────┘     └──────────┘     └──────────┘
                       │
                       │  (while Pending)
                       ├── Update header  (PUT /{company}/{id})
                       ├── Update lines   (PUT /{company}/{id}/lines)
                       ├── Print boxes    (PUT /{company}/{id}/box)
                       ├── Log box edits  (POST /box-edit-log)
                       └── Delete         (DELETE /{company}/{id})
```

### Typical Frontend Workflow

1. **Create** an RTV via `POST /rtv/{company}` with header + lines.
2. **View/List** RTVs via `GET /rtv/{company}` (paginated).
3. **Edit** header fields via `PUT /rtv/{company}/{id}`.
4. **Edit** line items via `PUT /rtv/{company}/{id}/lines` (full replacement).
5. **Print** each box via `PUT /rtv/{company}/{id}/box` — this generates the `box_id`.
6. **Log edits** to already-printed boxes via `POST /rtv/box-edit-log`.
7. **Approve** the RTV via `PUT /rtv/{company}/{id}/approve`, optionally passing final corrections in header/lines/boxes.
8. **Export** filtered records to Excel via `GET /rtv/export?company=CFPL`.

---

## 4. Box ID Generation Logic

```
box_id = "{last 8 digits of epoch milliseconds}-{box_number}"
```

**Example:** At epoch `1725350123456` ms:
- Box 1 → `50123456-1`
- Box 2 → `50123456-2`
- Box 3 → `50123456-3`

**Key rules:**
- `box_id` is `NULL` when a box is first created during approval.
- `box_id` is only generated when the **Print** endpoint (`PUT /{company}/{id}/box`) is called.
- Once assigned, a `box_id` is **never regenerated** — subsequent upserts preserve it.

---

## 5. Database Tables

Tables are company-prefixed:

| Company | Header Table     | Lines Table     | Boxes Table     |
|---------|------------------|-----------------|-----------------|
| CFPL    | `cfpl_rtv_header`| `cfpl_rtv_lines`| `cfpl_rtv_boxes`|
| CDPL    | `cdpl_rtv_header`| `cdpl_rtv_lines`| `cdpl_rtv_boxes`|

**Global table (shared):**

| Table            | Purpose                       |
|------------------|-------------------------------|
| `box_edit_logs`  | Audit log for box field edits |

### box_edit_logs Columns

| Column           | Description                              |
|------------------|------------------------------------------|
| `email_id`       | Who made the edit                        |
| `description`    | Auto-generated: `"Changed {field} from '{old}' to '{new}'"` |
| `transaction_no` | The `rtv_id` string                      |
| `box_id`         | The box's printed ID                     |
| `field_name`     | Which field was changed                  |
| `old_value`      | Previous value                           |
| `new_value`      | New value                                |
| `edited_at`      | UTC timestamp of the edit                |

---

## 6. Important Notes

1. **`rtv_id` (path param) is the DB `id` (integer), not the `rtv_id` string.** The `rtv_id` in URL paths refers to the auto-increment primary key (`id` column), not the `"RTV-YYYYMMDDHHmmSS"` string.

2. **Date format for filters is `DD-MM-YYYY`.** Both `from_date` and `to_date` query params expect this format. Invalid format returns `400`.

3. **`value` auto-computation:** If `value` is `"0"` (or not provided), the backend computes it as `qty * rate`.

4. **`material_type` and `uom` are auto-uppercased.** The frontend can send any case; the backend normalizes to uppercase.

5. **Lines update is a full replacement.** `PUT /{company}/{id}/lines` deletes all existing lines and inserts the new set. Boxes are not affected.

6. **Box ↔ Line linking:** Boxes are linked to lines by matching `article_description` (box) to `item_description` (line). The `rtv_line_id` FK is resolved automatically.

7. **Approval is a one-shot operation.** It sets status to `"Approved"`, records the approver and timestamp, and optionally applies final corrections to header/lines/boxes in a single call.

8. **Export highlights edits.** The Excel export cross-references the `box_edit_logs` table and highlights edited box cells with a red background (`#FEE2E2`).

9. **Deletion is cascading.** Deleting an RTV removes its header, all lines, and all boxes.

10. **Sort validation.** Invalid `sort_by` values are silently replaced with `created_ts`.

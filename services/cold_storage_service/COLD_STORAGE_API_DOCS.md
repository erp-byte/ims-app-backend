# Cold Storage API Documentation

Base URL: `/cold-storage`

---

## 1. List Cold Storage Records

**`GET /cold-storage`**

Returns a paginated, filterable, searchable list of cold storage stock records.

### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | int | `1` | Page number (min: 1) |
| `per_page` | int | `20` | Records per page (min: 1, max: 500) |
| `group_name` | string | — | Exact match filter (e.g. `"Dry Dates"`) |
| `storage_location` | string | — | Exact match filter (e.g. `"Savla"`) |
| `exporter` | string | — | Exact match filter (e.g. `"AL BARAKAH"`) |
| `item_mark` | string | — | Partial match filter (e.g. `"POWDER"` matches `"WHITE POWDER"`) |
| `search` | string | — | Global search across: item_description, group_name, exporter, item_mark, inward_no, lot_no, vakkal, unit, storage_location |
| `from_date` | string | — | Start date filter on inward_dt (`YYYY-MM-DD`) |
| `to_date` | string | — | End date filter on inward_dt (`YYYY-MM-DD`) |
| `sort_by` | string | `"id"` | Sort column. Allowed: `id`, `inward_dt`, `group_name`, `storage_location`, `exporter`, `total_inventory_kgs`, `value`, `created_at`, `item_description` |
| `sort_order` | string | `"desc"` | `asc` or `desc` |

### Example Request

```
GET /cold-storage?page=1&per_page=10&group_name=Dry Dates&sort_by=value&sort_order=desc
```

### Response `200 OK`

```json
{
  "records": [
    {
      "id": 2,
      "inward_dt": "2023-12-06",
      "unit": "D-39",
      "inward_no": "GR14501",
      "item_mark": "BASOOR=OMAN DRY DATES",
      "vakkal": "GRADE 2",
      "lot_no": "77517",
      "no_of_cartons": 49.0,
      "weight_kg": 7.0,
      "total_inventory_kgs": 343.0,
      "group_name": "Dry Dates",
      "item_description": "DRY DATES-GRADE 2",
      "storage_location": "Savla",
      "exporter": "NAKHEEL",
      "last_purchase_rate": 241.0,
      "value": 82663.0,
      "created_at": "2026-02-18T12:46:25.597185",
      "updated_at": "2026-02-18T12:46:25.597185"
    }
  ],
  "total": 150,
  "page": 1,
  "per_page": 10,
  "total_pages": 15
}
```

---

## 2. Get Single Record

**`GET /cold-storage/{record_id}`**

### Path Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `record_id` | int | Record ID |

### Response `200 OK`

```json
{
  "id": 1,
  "inward_dt": "2023-07-24",
  "unit": "D-39",
  "inward_no": "GR5903",
  "item_mark": "WHITE POWDER",
  "vakkal": "V-2",
  "lot_no": "56059",
  "no_of_cartons": 39.0,
  "weight_kg": 10.0,
  "total_inventory_kgs": 390.0,
  "group_name": "Date Powder",
  "item_description": "10KG AL BARAKAH DATE POWDER V2",
  "storage_location": "Savla",
  "exporter": "AL BARAKAH",
  "last_purchase_rate": 215.0,
  "value": 83850.0,
  "created_at": "2026-02-18T12:46:25.597185",
  "updated_at": "2026-02-18T12:46:25.597185"
}
```

### Error `404`

```json
{ "detail": "Record not found" }
```

---

## 3. Create Single Record

**`POST /cold-storage`**

### Request Body

All fields are optional. Send only the fields you have data for.

```json
{
  "inward_dt": "2024-01-15",
  "unit": "D-39",
  "inward_no": "GR20001",
  "item_mark": "AJWA DATES",
  "vakkal": "GRADE 1",
  "lot_no": "90001",
  "no_of_cartons": 100.00,
  "weight_kg": 5.000,
  "total_inventory_kgs": 500.00,
  "group_name": "Premium Dates",
  "item_description": "5KG AJWA DATES GRADE 1",
  "storage_location": "Savla",
  "exporter": "AL MADINA",
  "last_purchase_rate": 850.00,
  "value": 425000.00
}
```

### Request Body Fields

| Field | Type | Description |
|-------|------|-------------|
| `inward_dt` | string (date) | Inward date (`YYYY-MM-DD`) |
| `unit` | string | Unit code (e.g. `"D-39"`) |
| `inward_no` | string | GR number (e.g. `"GR5903"`) |
| `item_mark` | string | Item mark / name |
| `vakkal` | string | Vakkal / grade |
| `lot_no` | string | Lot number |
| `no_of_cartons` | number | Number of cartons (decimal, 2 places) |
| `weight_kg` | number | Weight per carton in kg (decimal, 3 places) |
| `total_inventory_kgs` | number | Total inventory in kg (decimal, 2 places) |
| `group_name` | string | Product group (e.g. `"Dry Dates"`, `"Date Powder"`) |
| `item_description` | string | Full item description |
| `storage_location` | string | Storage location name |
| `exporter` | string | Exporter / supplier name |
| `last_purchase_rate` | number | Last purchase rate per unit (decimal, 2 places) |
| `value` | number | Total value (decimal, 2 places) |

### Response `201 Created`

Returns the full created record (same shape as Get Single Record).

---

## 4. Update Record

**`PUT /cold-storage/{record_id}`**

Partial update — only send the fields you want to change. `updated_at` is set automatically.

### Path Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `record_id` | int | Record ID |

### Request Body

```json
{
  "no_of_cartons": 45.00,
  "total_inventory_kgs": 315.00,
  "value": 75915.00
}
```

### Response `200 OK`

Returns the full updated record.

### Error `400`

```json
{ "detail": "No fields to update" }
```

### Error `404`

```json
{ "detail": "Record not found" }
```

---

## 5. Delete Record

**`DELETE /cold-storage/{record_id}`**

### Path Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `record_id` | int | Record ID |

### Response `200 OK`

```json
{
  "success": true,
  "message": "Record deleted",
  "id": 1
}
```

### Error `404`

```json
{ "detail": "Record not found" }
```

---

## 6. Bulk Create Records

**`POST /cold-storage/bulk`**

Insert multiple records at once (e.g. from CSV/JSON upload).

### Request Body

```json
{
  "records": [
    {
      "inward_dt": "2024-01-15",
      "unit": "D-39",
      "inward_no": "GR20001",
      "item_mark": "AJWA DATES",
      "vakkal": "GRADE 1",
      "lot_no": "90001",
      "no_of_cartons": 100.00,
      "weight_kg": 5.000,
      "total_inventory_kgs": 500.00,
      "group_name": "Premium Dates",
      "item_description": "5KG AJWA DATES GRADE 1",
      "storage_location": "Savla",
      "exporter": "AL MADINA",
      "last_purchase_rate": 850.00,
      "value": 425000.00
    },
    {
      "inward_dt": "2024-01-16",
      "unit": "D-40",
      "inward_no": "GR20002",
      "item_mark": "MEDJOOL",
      "group_name": "Premium Dates",
      "no_of_cartons": 50.00,
      "weight_kg": 5.000,
      "total_inventory_kgs": 250.00
    }
  ]
}
```

### Response `201 Created`

```json
{
  "status": "created",
  "records_created": 2
}
```

---

## 7. Get Summary (Aggregated by Group)

**`GET /cold-storage/summary`**

Returns inventory totals aggregated by `group_name`, with optional filters.

### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `group_name` | string | — | Filter to a specific group |
| `storage_location` | string | — | Filter by storage location |
| `exporter` | string | — | Filter by exporter |

### Example Request

```
GET /cold-storage/summary?storage_location=Savla
```

### Response `200 OK`

```json
{
  "summary": [
    {
      "group_name": "Dry Dates",
      "total_records": 85,
      "total_cartons": 4250.0,
      "total_inventory_kgs": 29750.0,
      "total_value": 7169750.0
    },
    {
      "group_name": "Date Powder",
      "total_records": 20,
      "total_cartons": 780.0,
      "total_inventory_kgs": 7800.0,
      "total_value": 1677000.0
    }
  ],
  "grand_total_records": 105,
  "grand_total_inventory_kgs": 37550.0,
  "grand_total_value": 8846750.0
}
```

---

## Common Error Responses

| Status | Description |
|--------|-------------|
| `400` | Bad request (e.g. no fields to update) |
| `404` | Record not found |
| `422` | Validation error (invalid field types/values) |
| `500` | Internal server error |

### Validation Error Example (`422`)

```json
{
  "detail": [
    {
      "loc": ["body", "no_of_cartons"],
      "msg": "Input should be a valid decimal",
      "type": "decimal_parsing"
    }
  ]
}
```

---

## TypeScript Interfaces (Frontend Reference)

```typescript
// ── Record shape ──

interface ColdStorageRecord {
  id: number;
  inward_dt: string | null;       // "YYYY-MM-DD"
  unit: string | null;
  inward_no: string | null;
  item_mark: string | null;
  vakkal: string | null;
  lot_no: string | null;
  no_of_cartons: number | null;
  weight_kg: number | null;
  total_inventory_kgs: number | null;
  group_name: string | null;
  item_description: string | null;
  storage_location: string | null;
  exporter: string | null;
  last_purchase_rate: number | null;
  value: number | null;
  created_at: string | null;      // ISO datetime
  updated_at: string | null;      // ISO datetime
}

// ── Create / Update payloads ──

interface ColdStorageCreatePayload {
  inward_dt?: string;
  unit?: string;
  inward_no?: string;
  item_mark?: string;
  vakkal?: string;
  lot_no?: string;
  no_of_cartons?: number;
  weight_kg?: number;
  total_inventory_kgs?: number;
  group_name?: string;
  item_description?: string;
  storage_location?: string;
  exporter?: string;
  last_purchase_rate?: number;
  value?: number;
}

type ColdStorageUpdatePayload = Partial<ColdStorageCreatePayload>;

// ── List response ──

interface ColdStorageListResponse {
  records: ColdStorageRecord[];
  total: number;
  page: number;
  per_page: number;
  total_pages: number;
}

// ── Bulk create ──

interface BulkCreatePayload {
  records: ColdStorageCreatePayload[];
}

interface BulkCreateResponse {
  status: string;
  records_created: number;
}

// ── Delete response ──

interface ColdStorageDeleteResponse {
  success: boolean;
  message: string;
  id: number | null;
}

// ── Summary ──

interface GroupSummaryItem {
  group_name: string;
  total_records: number;
  total_cartons: number;
  total_inventory_kgs: number;
  total_value: number;
}

interface ColdStorageSummaryResponse {
  summary: GroupSummaryItem[];
  grand_total_records: number;
  grand_total_inventory_kgs: number;
  grand_total_value: number;
}

// ── List query params ──

interface ColdStorageListParams {
  page?: number;
  per_page?: number;
  group_name?: string;
  storage_location?: string;
  exporter?: string;
  item_mark?: string;
  search?: string;
  from_date?: string;            // "YYYY-MM-DD"
  to_date?: string;              // "YYYY-MM-DD"
  sort_by?: 'id' | 'inward_dt' | 'group_name' | 'storage_location'
          | 'exporter' | 'total_inventory_kgs' | 'value'
          | 'created_at' | 'item_description';
  sort_order?: 'asc' | 'desc';
}
```

# Looker Routes

This document describes the Looker API routes implemented in this service.

## Base Prefix

- App prefix from `main.py`: `/looker`
- Looker router prefix from `modules/looker/routes/looker_routes.py`: `/convert`
- Full route base: `/looker/convert`

## Endpoint Summary

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/looker/convert/measure` | Convert a LookML measure to DAX |
| `POST` | `/looker/convert/dimension` | Convert a LookML dimension to DAX |
| `POST` | `/looker/convert/expression` | Convert a generic Looker expression to DAX |
| `POST` | `/looker/convert/batch` | Convert multiple measure/dimension items in one request |
| `GET` | `/looker/convert/rag/status` | Inspect Looker RAG readiness/configuration |
| `POST` | `/looker/convert/rag/test` | Test RAG retrieval for an expression (no conversion) |

## Request/Response Schemas

### 1) `POST /looker/convert/measure`

Request body (`LookerMeasureRequest`):

- Required:
  - `measure_name` (`string`)
  - `measure_type` (`string`) expected values: `count`, `count_distinct`, `sum`, `average`, `min`, `max`, `number`
  - `view_name` (`string`)
  - `table_name` (`string`)
- Optional:
  - `sql_expression` (`string | null`)
  - `column_mappings` (`object<string,string> | null`)
  - `dependencies` (`LookerDependency[] | null`)
  - `filters` (`object<string,string> | null`)
  - `use_rag` (`boolean`, default `true`)

Response (`LookerMeasureResponse`):

- `success`, `dax_expression`, `original_expression`, `measure_name`, `measure_type`, `confidence`, `used_llm`, `used_rag`, `warnings`, `error`

Example:

```json
{
  "measure_name": "total_revenue",
  "measure_type": "sum",
  "sql_expression": "${TABLE}.revenue",
  "view_name": "orders",
  "table_name": "Orders",
  "use_rag": true
}
```

### 2) `POST /looker/convert/dimension`

Request body (`LookerDimensionRequest`):

- Required:
  - `dimension_name` (`string`)
  - `dimension_type` (`string`) expected values: `string`, `number`, `yesno`, `date`, `time`, `tier`
  - `view_name` (`string`)
  - `table_name` (`string`)
- Optional:
  - `sql_expression` (`string | null`)
  - `column_mappings` (`object<string,string> | null`)
  - `use_rag` (`boolean`, default `true`)

Response (`LookerDimensionResponse`):

- `success`, `dax_expression`, `original_expression`, `dimension_name`, `dimension_type`, `confidence`, `used_llm`, `used_rag`, `warnings`, `error`

Example:

```json
{
  "dimension_name": "order_status",
  "dimension_type": "string",
  "sql_expression": "CASE WHEN ${TABLE}.status = 'C' THEN 'Closed' ELSE 'Open' END",
  "view_name": "orders",
  "table_name": "Orders",
  "use_rag": true
}
```

### 3) `POST /looker/convert/expression`

Request body (`LookerExpressionRequest`):

- Required:
  - `expression` (`string`)
  - `view_name` (`string`)
  - `table_name` (`string`)
- Optional:
  - `expression_type` (`string`, default `measure`)
  - `column_mappings` (`object<string,string> | null`)
  - `dependencies` (`LookerDependency[] | null`)
  - `use_rag` (`boolean`, default `true`)

Response (`LookerExpressionResponse`):

- `success`, `dax_expression`, `original_expression`, `expression_type`, `confidence`, `used_llm`, `used_rag`, `warnings`, `error`

Example:

```json
{
  "expression": "${revenue} / NULLIF(${orders}, 0)",
  "expression_type": "measure",
  "view_name": "orders",
  "table_name": "Orders"
}
```

### 4) `POST /looker/convert/batch`

Request body (`LookerBatchRequest`):

- Required:
  - `items` (`LookerBatchConversionItem[]`)
  - `view_name` (`string`)
  - `table_name` (`string`)
- Optional:
  - `column_mappings` (`object<string,string> | null`)
  - `use_rag` (`boolean`, default `true`)
  - `resolve_dependencies` (`boolean`, default `true`)

`LookerBatchConversionItem`:

- Required:
  - `name` (`string`)
  - `item_type` (`string`) expected `measure` or `dimension`
- Optional:
  - `measure_type` (`string`)
  - `dimension_type` (`string`)
  - `sql_expression` (`string | null`)
  - `filters` (`object<string,string> | null`)

Response (`LookerBatchResponse`):

- `success`, `total_items`, `successful_conversions`, `failed_conversions`, `items`, `errors`

Example:

```json
{
  "view_name": "orders",
  "table_name": "Orders",
  "resolve_dependencies": true,
  "use_rag": true,
  "items": [
    {
      "name": "total_revenue",
      "item_type": "measure",
      "measure_type": "sum",
      "sql_expression": "${TABLE}.revenue"
    },
    {
      "name": "order_status",
      "item_type": "dimension",
      "dimension_type": "string",
      "sql_expression": "${TABLE}.status"
    }
  ]
}
```

### 5) `GET /looker/convert/rag/status`

No request body.

Returns:

- `status` (`initialized | not_initialized | error`)
- `vector_search_enabled`
- `patterns_loaded`
- `mistakes_loaded`
- `function_mappings_loaded`
- `knowledge_base_path`
- `collection_name`

### 6) `POST /looker/convert/rag/test`

This route uses query parameters (not JSON body):

- `expression` (`string`, required)
- `expression_type` (`string`, default `measure`)
- `k` (`int`, default `3`)

Example:

```bash
curl -X POST "https://<host>/looker/convert/rag/test?expression=SUM%28price%29&expression_type=measure&k=3"
```

Returns:

- `expression`, `expression_type`, `examples_count`, `examples`, `formatted_prompt_section`, `retrieval_method`

## Behavior Notes

- Conversion endpoints call LLM + optional RAG examples.
- `use_rag` defaults to `true`.
- Batch conversion can resolve dependencies by processing measures before dimensions.
- For invalid request payloads, FastAPI returns `422`.
- Conversion handlers generally return `200` with `success: false` and `error` when runtime conversion fails.

## Current Route Gap

- There is no `GET /looker/health` route in current Looker module routes.
- Health checks for Looker availability should currently use either:
  - global `GET /health`
  - `GET /looker/convert/rag/status` for Looker RAG-specific status

view: inventory_items {
  sql_table_name: `bigquery-public-data.thelook_ecommerce.inventory_items` ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: product_id {
    type: number
    sql: ${TABLE}.product_id ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
  }

  dimension_group: sold {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.sold_at ;;
  }

  dimension: cost {
    type: number
    sql: ${TABLE}.cost ;;
    value_format_name: usd
  }

  dimension: product_category {
    type: string
    sql: ${TABLE}.product_category ;;
  }

  dimension: product_name {
    type: string
    sql: ${TABLE}.product_name ;;
  }

  dimension: product_brand {
    type: string
    sql: ${TABLE}.product_brand ;;
  }

  dimension: product_retail_price {
    type: number
    sql: ${TABLE}.product_retail_price ;;
    value_format_name: usd
  }

  dimension: product_department {
    type: string
    sql: ${TABLE}.product_department ;;
  }

  dimension: product_sku {
    type: string
    sql: ${TABLE}.product_sku ;;
  }

  dimension: product_distribution_center_id {
    type: number
    sql: ${TABLE}.product_distribution_center_id ;;
  }

  dimension: is_sold {
    type: yesno
    sql: ${sold_raw} IS NOT NULL ;;
    description: "Whether the inventory item has been sold"
  }

  dimension: days_in_inventory {
    type: number
    sql: DATE_DIFF(COALESCE(${sold_raw}, CURRENT_TIMESTAMP()), ${created_raw}, DAY) ;;
    description: "Number of days item was/is in inventory"
  }

  # Measures
  measure: count {
    type: count
    drill_fields: [id, product_name, product_category]
  }

  measure: sold_count {
    type: count
    filters: [is_sold: "Yes"]
    description: "Count of sold inventory items"
  }

  measure: unsold_count {
    type: count
    filters: [is_sold: "No"]
    description: "Count of unsold inventory items"
  }

  measure: total_cost {
    type: sum
    sql: ${cost} ;;
    value_format_name: usd
  }

  measure: average_days_in_inventory {
    type: average
    sql: ${days_in_inventory} ;;
    value_format: "0.0"
    description: "Average days items spend in inventory"
  }

  measure: sell_through_rate {
    type: number
    sql: ${sold_count} / NULLIF(${count}, 0) * 100 ;;
    value_format: "0.00\%"
    description: "Percentage of inventory that has been sold"
  }
}

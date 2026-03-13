view: products {
  sql_table_name: `bigquery-public-data.thelook_ecommerce.products` ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: cost {
    type: number
    sql: ${TABLE}.cost ;;
    value_format_name: usd
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: brand {
    type: string
    sql: ${TABLE}.brand ;;
  }

  dimension: retail_price {
    type: number
    sql: ${TABLE}.retail_price ;;
    value_format_name: usd
  }

  dimension: department {
    type: string
    sql: ${TABLE}.department ;;
  }

  dimension: sku {
    type: string
    sql: ${TABLE}.sku ;;
  }

  dimension: distribution_center_id {
    type: number
    sql: ${TABLE}.distribution_center_id ;;
  }

  dimension: profit_margin {
    type: number
    sql: (${retail_price} - ${cost}) / NULLIF(${retail_price}, 0) ;;
    value_format: "0.00%"
    description: "Profit margin as percentage of retail price"
  }

  # Measures
  measure: count {
    type: count
    drill_fields: [id, name, category, brand]
  }

  measure: average_cost {
    type: average
    sql: ${cost} ;;
    value_format_name: usd
  }

  measure: average_retail_price {
    type: average
    sql: ${retail_price} ;;
    value_format_name: usd
  }

  measure: total_cost {
    type: sum
    sql: ${cost} ;;
    value_format_name: usd
  }

  measure: total_retail_value {
    type: sum
    sql: ${retail_price} ;;
    value_format_name: usd
    description: "Sum of all product retail prices"
  }

  measure: average_profit_margin {
    type: average
    sql: ${profit_margin} ;;
    value_format: "0.00%"
    description: "Average profit margin across products"
  }
}

view: event_session_funnel {
  derived_table: {
    sql:
      SELECT
        session_id,
        MAX(CASE WHEN event_type = 'Home' THEN 1 ELSE 0 END) as visited_home,
        MAX(CASE WHEN event_type = 'Category' THEN 1 ELSE 0 END) as visited_category,
        MAX(CASE WHEN event_type = 'Product' THEN 1 ELSE 0 END) as viewed_product,
        MAX(CASE WHEN event_type = 'Cart' THEN 1 ELSE 0 END) as added_to_cart,
        MAX(CASE WHEN event_type = 'Purchase' THEN 1 ELSE 0 END) as completed_purchase
      FROM `bigquery-public-data.thelook_ecommerce.events`
      GROUP BY session_id
    ;;
  }

  dimension: session_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: visited_home {
    type: yesno
    sql: ${TABLE}.visited_home = 1 ;;
  }

  dimension: visited_category {
    type: yesno
    sql: ${TABLE}.visited_category = 1 ;;
  }

  dimension: viewed_product {
    type: yesno
    sql: ${TABLE}.viewed_product = 1 ;;
  }

  dimension: added_to_cart {
    type: yesno
    sql: ${TABLE}.added_to_cart = 1 ;;
  }

  dimension: completed_purchase {
    type: yesno
    sql: ${TABLE}.completed_purchase = 1 ;;
  }

  dimension: funnel_stage {
    type: string
    sql: CASE
      WHEN ${completed_purchase} THEN 'Purchase'
      WHEN ${added_to_cart} THEN 'Cart'
      WHEN ${viewed_product} THEN 'Product View'
      WHEN ${visited_category} THEN 'Category'
      WHEN ${visited_home} THEN 'Home'
      ELSE 'Unknown'
    END ;;
    description: "Furthest stage reached in funnel"
  }

  # Measures
  measure: count {
    type: count
    drill_fields: [session_id, funnel_stage]
  }

  measure: home_visitors {
    type: count
    filters: [visited_home: "Yes"]
    description: "Sessions that visited home page"
  }

  measure: category_visitors {
    type: count
    filters: [visited_category: "Yes"]
    description: "Sessions that visited category page"
  }

  measure: product_viewers {
    type: count
    filters: [viewed_product: "Yes"]
    description: "Sessions that viewed a product"
  }

  measure: cart_adders {
    type: count
    filters: [added_to_cart: "Yes"]
    description: "Sessions that added to cart"
  }

  measure: purchasers {
    type: count
    filters: [completed_purchase: "Yes"]
    description: "Sessions that completed purchase"
  }

  measure: home_to_category_rate {
    type: number
    sql: ${category_visitors} / NULLIF(${home_visitors}, 0) * 100 ;;
    value_format: "0.00\%"
    description: "Conversion from home to category"
  }

  measure: category_to_product_rate {
    type: number
    sql: ${product_viewers} / NULLIF(${category_visitors}, 0) * 100 ;;
    value_format: "0.00\%"
    description: "Conversion from category to product"
  }

  measure: product_to_cart_rate {
    type: number
    sql: ${cart_adders} / NULLIF(${product_viewers}, 0) * 100 ;;
    value_format: "0.00\%"
    description: "Conversion from product to cart"
  }

  measure: cart_to_purchase_rate {
    type: number
    sql: ${purchasers} / NULLIF(${cart_adders}, 0) * 100 ;;
    value_format: "0.00\%"
    description: "Conversion from cart to purchase"
  }

  measure: overall_conversion_rate {
    type: number
    sql: ${purchasers} / NULLIF(${home_visitors}, 0) * 100 ;;
    value_format: "0.00\%"
    description: "Overall funnel conversion rate"
  }
}

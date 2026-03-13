view: users {
  sql_table_name: `bigquery-public-data.thelook_ecommerce.users` ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: full_name {
    type: string
    sql: CONCAT(${first_name}, ' ', ${last_name}) ;;
    description: "User's full name"
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: age {
    type: number
    sql: ${TABLE}.age ;;
  }

  dimension: age_group {
    type: tier
    tiers: [18, 25, 35, 45, 55, 65]
    style: integer
    sql: ${age} ;;
    description: "Age grouped into tiers"
  }

  dimension: gender {
    type: string
    sql: ${TABLE}.gender ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
  }

  dimension: zip {
    type: zipcode
    sql: ${TABLE}.zip ;;
  }

  dimension: latitude {
    type: number
    sql: ${TABLE}.latitude ;;
  }

  dimension: longitude {
    type: number
    sql: ${TABLE}.longitude ;;
  }

  dimension: traffic_source {
    type: string
    sql: ${TABLE}.traffic_source ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
  }

  # Measures
  measure: count {
    type: count
    drill_fields: [id, full_name, email, city, state]
  }

  measure: average_age {
    type: average
    sql: ${age} ;;
    value_format: "0.0"
  }

  measure: new_users_count {
    type: count_distinct
    sql: ${id} ;;
    description: "Count of unique users"
  }
}

view: events {
  sql_table_name: `bigquery-public-data.thelook_ecommerce.events` ;;

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: session_id {
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension: sequence_number {
    type: number
    sql: ${TABLE}.sequence_number ;;
  }

  dimension_group: created {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.created_at ;;
  }

  dimension: ip_address {
    type: string
    sql: ${TABLE}.ip_address ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: postal_code {
    type: string
    sql: ${TABLE}.postal_code ;;
  }

  dimension: browser {
    type: string
    sql: ${TABLE}.browser ;;
  }

  dimension: traffic_source {
    type: string
    sql: ${TABLE}.traffic_source ;;
  }

  dimension: uri {
    type: string
    sql: ${TABLE}.uri ;;
  }

  dimension: event_type {
    type: string
    sql: ${TABLE}.event_type ;;
  }

  # Measures
  measure: count {
    type: count
    drill_fields: [id, event_type, traffic_source]
  }

  measure: unique_sessions {
    type: count_distinct
    sql: ${session_id} ;;
    description: "Count of unique sessions"
  }

  measure: unique_users {
    type: count_distinct
    sql: ${user_id} ;;
    description: "Count of unique users"
  }

  measure: events_per_session {
    type: number
    sql: ${count} / NULLIF(${unique_sessions}, 0) ;;
    value_format: "0.00"
    description: "Average number of events per session"
  }

  measure: page_views {
    type: count
    filters: [event_type: "Page View"]
    description: "Count of page view events"
  }

  measure: product_views {
    type: count
    filters: [event_type: "Product"]
    description: "Count of product view events"
  }

  measure: cart_additions {
    type: count
    filters: [event_type: "Cart"]
    description: "Count of cart addition events"
  }

  measure: purchases {
    type: count
    filters: [event_type: "Purchase"]
    description: "Count of purchase events"
  }

  measure: conversion_rate {
    type: number
    sql: ${purchases} / NULLIF(${unique_sessions}, 0) * 100 ;;
    value_format: "0.00\%"
    description: "Purchase conversion rate"
  }
}

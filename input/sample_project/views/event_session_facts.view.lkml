view: event_session_facts {
  derived_table: {
    sql:
      SELECT
        session_id,
        MIN(created_at) as session_start,
        MAX(created_at) as session_end,
        COUNT(*) as event_count,
        COUNT(DISTINCT event_type) as unique_event_types,
        MAX(CASE WHEN event_type = 'Purchase' THEN 1 ELSE 0 END) as has_purchase
      FROM `bigquery-public-data.thelook_ecommerce.events`
      GROUP BY session_id
    ;;
  }

  dimension: session_id {
    primary_key: yes
    type: string
    sql: ${TABLE}.session_id ;;
  }

  dimension_group: session_start {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.session_start ;;
  }

  dimension_group: session_end {
    type: time
    timeframes: [raw, time, date, week, month, quarter, year]
    sql: ${TABLE}.session_end ;;
  }

  dimension: event_count {
    type: number
    sql: ${TABLE}.event_count ;;
  }

  dimension: unique_event_types {
    type: number
    sql: ${TABLE}.unique_event_types ;;
  }

  dimension: has_purchase {
    type: yesno
    sql: ${TABLE}.has_purchase = 1 ;;
    description: "Whether session resulted in a purchase"
  }

  dimension: session_duration_seconds {
    type: number
    sql: TIMESTAMP_DIFF(${session_end_raw}, ${session_start_raw}, SECOND) ;;
    description: "Session duration in seconds"
  }

  dimension: session_duration_tier {
    type: tier
    tiers: [30, 60, 120, 300, 600]
    style: integer
    sql: ${session_duration_seconds} ;;
    description: "Session duration grouped into tiers"
  }

  # Measures
  measure: count {
    type: count
    drill_fields: [session_id, session_start_time, event_count]
  }

  measure: average_event_count {
    type: average
    sql: ${event_count} ;;
    value_format: "0.00"
    description: "Average events per session"
  }

  measure: average_session_duration {
    type: average
    sql: ${session_duration_seconds} ;;
    value_format: "0.0"
    description: "Average session duration in seconds"
  }

  measure: sessions_with_purchase {
    type: count
    filters: [has_purchase: "Yes"]
    description: "Number of sessions that resulted in a purchase"
  }

  measure: session_conversion_rate {
    type: number
    sql: ${sessions_with_purchase} / NULLIF(${count}, 0) * 100 ;;
    value_format: "0.00\%"
    description: "Percentage of sessions that convert to purchase"
  }
}

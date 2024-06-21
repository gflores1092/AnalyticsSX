(
    SELECT
      STRUCT (
        --working_seconds,
        --SAFE_DIVIDE(working_seconds, 60) AS working_minutes,
        --SAFE_DIVIDE(working_seconds, 60*60) AS working_hours,
        --working_days,
        --seconds,
        --minutes,
        hours--,
        --days
      )
    FROM (
      SELECT
        ((week_diff * number_of_working_days_in_week) + (x_work_days_in_week - y_work_days_in_week))
          * (day_end_hour - day_start_hour) * 60 * 60 + same_day_diff_norm AS working_seconds,
        (week_diff * number_of_working_days_in_week) + (x_work_days_in_week - y_work_days_in_week) AS working_days,
        TIMESTAMP_DIFF(x, y, SECOND) AS seconds,
        TIMESTAMP_DIFF(x, y, MINUTE) AS minutes,
        TIMESTAMP_DIFF(x, y, HOUR) AS hours,
        DATE_DIFF(DATE(x), DATE(y), DAY) AS days
      FROM (
        SELECT
          week_diff,
          same_day_diff_norm,
          number_of_working_days_in_week,
          day_diff * (day_end_hour - day_start_hour) * 60 * 60 + same_day_diff_norm AS work_hours_second_diff,
          COUNT(IF(working_day_of_week <= x_day_of_week, 1, NULL)) AS x_work_days_in_week,
          COUNT(IF(working_day_of_week <= y_day_of_week, 1, NULL)) AS y_work_days_in_week
        FROM (
          SELECT
            DATE_DIFF(DATE(x_norm), DATE(y_norm), DAY) AS day_diff,
            DATE_DIFF(DATE(x_norm), DATE(y_norm), WEEK) AS week_diff,
            EXTRACT(DAYOFWEEK FROM x_norm) AS x_day_of_week,
            EXTRACT(DAYOFWEEK FROM y_norm) AS y_day_of_week,
            TIME_DIFF(TIME(x_norm), TIME(y_norm), SECOND) AS same_day_diff_norm,
            ARRAY_LENGTH(working_days_of_week) AS number_of_working_days_in_week,
            working_day_of_week
          FROM (
            SELECT
              -- Start each work day at 00:00:00 and end each work day at day_end_hour-day_start_hour
              LEAST(GREATEST(TIMESTAMP_ADD(x, INTERVAL -day_start_hour HOUR), TIMESTAMP_TRUNC(x, DAY)), TIMESTAMP_ADD(TIMESTAMP_TRUNC(x, DAY), INTERVAL day_end_hour-day_start_hour HOUR)) AS x_norm,
              LEAST(GREATEST(TIMESTAMP_ADD(y, INTERVAL -day_start_hour HOUR), TIMESTAMP_TRUNC(y, DAY)), TIMESTAMP_ADD(TIMESTAMP_TRUNC(y, DAY), INTERVAL day_end_hour-day_start_hour HOUR)) AS y_norm,
              -- Make sure working days are in order
          )
          LEFT JOIN UNNEST(working_days_of_week) AS working_day_of_week

        )
        GROUP BY
          work_hours_second_diff,
          same_day_diff_norm,
          week_diff,
          number_of_working_days_in_week
      )
    )
  )

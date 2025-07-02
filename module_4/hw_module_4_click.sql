-- № 1. Поддерживать сырые логи событий (схема выше). 
--   Обратите внимание на то, что данные в этой таблице должны хранится 30 дней.
CREATE TABLE user_events (
	user_id UInt32,
	event_type String,
	points_spent UInt32,
	event_time DateTime
) ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time - INTERVAL 30 DAY;
 
-- № 2. Построить агрегированную таблицу. Храним агрегаты 180 дней, чтобы делать трендовый анализ:
--		уникальные пользователи по event_type и event_date
--      сумма потраченных баллов
--      количество действий

CREATE TABLE agg_user_events (
	event_date Date,
	event_type String,
	uniq_users_state AggregateFunction(uniq, UInt32),
	points_spent_state AggregateFunction(sum, UInt32),
	action_count_state AggregateFunction(count, UInt8)
) ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date - INTERVAL 180 DAY;


-- № 3. Сделать Materialized View, которая:
--   при вставке данных в таблицу сырых логов событий, будет обновлять агрегированную таблицу
--   использует sumState, uniqState, countState

CREATE MATERIALIZED VIEW mv_agg_user_events
TO agg_user_events AS 
SELECT toDate(event_time) AS event_date,
	   event_type,
	   uniqState(user_id) AS uniq_users_state,
	   sumState(points_spent) AS points_spent_state,
	   countState() AS action_count_state
FROM user_events
GROUP BY event_date, event_type;

-- № 4. Создать запрос, показывающий: Retention: сколько пользователей вернулись в течение следующих 7 дней.
WITH
first_event AS (
    SELECT
        user_id,
        MIN(toDate(event_time)) AS first_event_date
    FROM user_events
    GROUP BY user_id
),

users_day AS (
    SELECT
        first_event_date,
        COUNT(DISTINCT user_id) AS total_users_day_0
    FROM first_event
    GROUP BY first_event_date
),

returning_users AS (
    SELECT
        fe.user_id,
        fe.first_event_date,
        toDate(ue.event_time) AS return_date
    FROM first_event fe
    JOIN user_events ue ON fe.user_id = ue.user_id
    WHERE ue.event_time > toDateTime(fe.first_event_date)
      AND ue.event_time <= toDateTime(fe.first_event_date + INTERVAL 7 DAY)
      AND event_type = 'purchase'
),

returned_counts AS (
    SELECT
        first_event_date,
        COUNT(DISTINCT user_id) AS returned_in_7_days
    FROM returning_users
    GROUP BY first_event_date
)

SELECT 
    t.first_event_date,
    t.total_users_day_0,
    r.returned_in_7_days,
    if(t.total_users_day_0 = 0, 0, r.returned_in_7_days / t.total_users_day_0 * 100) AS retention_percent
FROM users_day t
LEFT JOIN returned_counts r ON t.first_event_date = r.first_event_date;


-- № 5. Создать запрос с группировками по быстрой аналитике по дням.
SELECT  event_date,
		event_type,
		uniqMerge(uniq_users_state) AS uniq_users,
	    sumMerge(points_spent_state) AS total_spent,
	    countMerge(action_count_state) AS total_action
FROM agg_user_events
GROUP BY event_date, event_type;


# Импорт библиотек
import psycopg2
from kafka import KafkaProducer
import json


# Создаем продьюсера
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Создаем подключение к PG
conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port="5435"
)
cursor = conn.cursor()

# Получаем данные из таблицы
cursor.execute(
    """SELECT id, username, event_type, extract(epoch FROM event_time) 
       FROM public.user_logins
       WHERE sent_to_kafka = False"""
    )
rows = cursor.fetchall()

# Отправляем данные в кафку и меняем значение флага sent_to_kafka
for row in rows:
    data = {
        "user": row[1],
        "event": row[2],
        "timestamp": float(row[3])
    }
    producer.send("pg_to_click", value=data)

    cursor.execute(f"UPDATE public.user_logins SET sent_to_kafka = TRUE WHERE id = {row[0]}")
    conn.commit()
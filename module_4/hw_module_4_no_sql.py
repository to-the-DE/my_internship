from pymongo import MongoClient
from datetime import datetime, timedelta
import json

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]
archived = db["archived_users"]

# Список документов
data = [
    {
        "user_id": 123,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 20, 10, 0, 0),
        "user_info": {
            "email": "user1@example.com",
            "registration_date": datetime(2023, 12, 1, 10, 0, 0)
        }
    },
    {
        "user_id": 124,
        "event_type": "login",
        "event_time": datetime(2024, 1, 21, 9, 30, 0),
        "user_info": {
            "email": "user2@example.com",
            "registration_date": datetime(2023, 12, 2, 12, 0, 0)
        }
    },
    {
        "user_id": 125,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 19, 14, 15, 0),
        "user_info": {
            "email": "user3@example.com",
            "registration_date": datetime(2023, 12, 3, 11, 45, 0)
        }
    },
    {
        "user_id": 126,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 20, 16, 0, 0),
        "user_info": {
            "email": "user4@example.com",
            "registration_date": datetime(2023, 12, 4, 9, 0, 0)
        }
    },
    {
        "user_id": 127,
        "event_type": "login",
        "event_time": datetime(2024, 1, 22, 10, 0, 0),
        "user_info": {
            "email": "user5@example.com",
            "registration_date": datetime(2023, 12, 5, 10, 0, 0)
        }
    },
    {
        "user_id": 128,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 22, 11, 30, 0),
        "user_info": {
            "email": "user6@example.com",
            "registration_date": datetime(2023, 12, 6, 13, 0, 0)
        }
    },
    {
        "user_id": 129,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 23, 15, 0, 0),
        "user_info": {
            "email": "user7@example.com",
            "registration_date": datetime(2023, 12, 7, 8, 0, 0)
        }
    },
    {
        "user_id": 130,
        "event_type": "login",
        "event_time": datetime(2024, 1, 23, 16, 45, 0),
        "user_info": {
            "email": "user8@example.com",
            "registration_date": datetime(2023, 12, 8, 10, 0, 0)
        }
    },
    {
        "user_id": 131,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 24, 12, 0, 0),
        "user_info": {
            "email": "user9@example.com",
            "registration_date": datetime(2023, 12, 9, 14, 0, 0)
        }
    },
    {
        "user_id": 132,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 24, 18, 30, 0),
        "user_info": {
            "email": "user10@example.com",
            "registration_date": datetime(2023, 12, 10, 10, 0, 0)
        }
    },
     {
         "user_id": 133,
         "event_type": "signup",
        "event_time": datetime(2025, 7, 1, 18, 30, 0),
        "user_info": {
            "email": "user10@example.com",
             "registration_date": datetime(2023, 12, 10, 10, 0, 0)
         }
     }
]

# Заливка данных в коллекцию
collection.insert_many(data)
print("✅ Данные успешно загружены в MongoDB")

# Текущая дата
current_date = datetime.now()

# Минимальная дата регистрации
min_register_date = current_date - timedelta(days=30)

# Минимальная дата активности
min_active_date = current_date - timedelta(days=14)

# Список данных для архивации
archived_data = []

for user in collection.find(
        {"user_info.registration_date": {"$lt": min_register_date},
         "event_time": {"$lt": min_active_date}}
                            ):
    archived_data.append(user)
# Добавляем данные в архивную коллекцию archived_users
archived.insert_many(archived_data)
print("✅ Пользователи успешно загружены в архивную коллекцию")

# Формируем отчет о заархивированных пользователях
report = {}
users_list = [dict_.get("user_id", "user_id не указан") for dict_ in archived_data]
report["date"] = str(current_date)[:10]
report["archived_users_count"] = len(users_list)
report["archived_user_ids"] = users_list

# Сохраняем отчет в JSON формате
with open(f'{str(current_date)[:10]}.json', 'w', encoding='utf-8') as file:
    json.dump(report, file, ensure_ascii=False, indent=4)
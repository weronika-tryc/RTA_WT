from kafka import KafkaConsumer, KafkaProducer
import json
import time
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest'
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

user_history = {}

print("System wykrywania anomalii aktywny...")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']
    
    current_time = time.time()
    
    if user_id not in user_history:
        user_history[user_id] = []
 
    user_history[user_id].append(current_time)
    
    user_history[user_id] = [t for t in user_history[user_id] if current_time - t <= 60]
    
    count = len(user_history[user_id])
    if count > 3:
        alert = {
            'timestamp': datetime.now().isoformat(),
            'user_id': user_id,
            'transaction_count': count,
            'message': 'ALARM: Wykryto serię szybkich transakcji!'
        }
        
        alert_producer.send('alerts', value=alert)
        
        print(f" ANOMALIA, Użytkownik {user_id} wykonał {count} transakcji w <60s!")

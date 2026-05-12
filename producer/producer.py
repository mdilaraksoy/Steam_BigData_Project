import pandas as pd
from kafka import KafkaProducer
import json
import time
import datetime
import os

# Kafka Ayarları
# BOOTSTRAP_SERVERS = 'localhost:9092' yerine bunu yaz:
BOOTSTRAP_SERVERS = 'kafka:29092'
TOPIC_NAME = 'steam_reviews'

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Kafka hazır olana kadar bekleyen fonksiyon
def get_producer():
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=[BOOTSTRAP_SERVERS],
                value_serializer=json_serializer,
                request_timeout_ms=120000 # Zaman aşımını uzattık
            )
            print("Kafka'ya başarıyla bağlandı!")
            return p
        except Exception as e:
            print(f"Kafka henüz hazır değil, bekleniyor... Hata: {e}")
            time.sleep(5)

producer = get_producer()

def stream_data():
    file_path = 'dataset/dataset.csv'
    if not os.path.exists(file_path):
        print(f"Hata: {file_path} bulunamadı!")
        return

    print("CSV dosyası okunuyor...")
    df = pd.read_csv(file_path).head(100000)
    
    print(f"Streaming başlıyor... Toplam {len(df)} kayıt...")
    
    for index, row in df.iterrows():
        message = {
            'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'app_id': str(row.get('app_id', 'unknown')),
            'review_id': int(index),
            'review_text': str(row.get('review', '')),
            'recommended': bool(row.get('recommended', True))
        }
        
        try:
            producer.send(TOPIC_NAME, message)
            if index % 100 == 0:
                print(f"Log: {index} mesaj gönderildi.")
            time.sleep(0.05) # Saniyede 20 mesaj 
        except Exception as e:
            print(f"Gönderim hatası: {e}")
            break

if __name__ == "__main__":
    stream_data()
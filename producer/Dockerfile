FROM python:3.9-slim
WORKDIR /app
# Gerekli kütüphaneleri yükleyelim
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Kodlarımızı içeri aktaralım
COPY . .
# Producer'ın çalışması için komut (Bir sonraki adımda producer.py'yi yazacağız)
CMD ["python", "producer.py"]
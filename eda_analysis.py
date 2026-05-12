from pyspark.sql import SparkSession

# Analiz için Spark oturumu
spark = SparkSession.builder \
    .appName("SteamEDA") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Delta Lake'den veriyi oku (Bronze Katmanı)
print("📊 Delta Lake (Bronze Katmanı) analiz ediliyor...")
# Yol olarak /app/delta_lake/bronze_reviews kullanıyoruz çünkü Docker burayı görüyor
df = spark.read.format("delta").load("/app/delta_lake/bronze_reviews")

# 1. Temel İstatistik: Toplam satır sayısı 
row_count = df.count()
print(f"\n✅ Toplam İşlenen Yorum Sayısı: {row_count}")

# 2. Olay Dağılımı: Tavsiye edilme oranları [cite: 37, 40]
print("\n📈 Tavsiye Edilme Dağılımı (Recommended):")
df.groupBy("recommended").count().show()

# 3. Şema Kontrolü: Veri tipleri doğru mu? [cite: 31, 32]
print("\n📋 Veri Şeması:")
df.printSchema()

# 4. Örnek Veriler
print("\n📝 İlk 5 Kayıt:")
df.show(5)
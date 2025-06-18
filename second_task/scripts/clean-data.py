from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, BooleanType, FloatType
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("Parquet ETL Result").getOrCreate()

source_path = "s3a://ht-data-storage/transactions_v2.csv"
target_path = "s3a://airflow-result-111/transactions_v2_clean.parquet"

try:
    print(f"Чтение данных из: {source_path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)

    print("Схема исходных данных:")
    df.printSchema()

    # Приведение типов
    df = df.withColumn("id", col("id").cast(IntegerType())) \
           .withColumn("age", col("age").cast(IntegerType())) \
           .withColumn("gender", col("gender").cast(StringType())) \
           .withColumn("job_type", col("job_type").cast(StringType())) \
           .withColumn("daily_social_media_time", col("daily_social_media_time").cast(FloatType())) \
           .withColumn("social_platform_preference", col("social_platform_preference").cast(StringType())) \
           .withColumn("number_of_notifications", col("number_of_notifications").cast(IntegerType())) \
           .withColumn("work_hours_per_day", col("work_hours_per_day").cast(FloatType())) \
           .withColumn("perceived_productivity_score", col("perceived_productivity_score").cast(FloatType())) \
           .withColumn("actual_productivity_score", col("actual_productivity_score").cast(FloatType())) \
           .withColumn("stress_level", col("stress_level").cast(FloatType())) \
           .withColumn("sleep_hours", col("sleep_hours").cast(FloatType())) \
           .withColumn("screen_time_before_sleep", col("screen_time_before_sleep").cast(FloatType())) \
           .withColumn("breaks_during_work", col("breaks_during_work").cast(IntegerType())) \
           .withColumn("uses_focus_apps", col("uses_focus_apps").cast(BooleanType())) \
           .withColumn("has_digital_wellbeing_enabled", col("has_digital_wellbeing_enabled").cast(BooleanType())) \
           .withColumn("coffee_consumption_per_day", col("coffee_consumption_per_day").cast(IntegerType())) \
           .withColumn("days_feeling_burnout_per_month", col("days_feeling_burnout_per_month").cast(IntegerType())) \
           .withColumn("weekly_offline_hours", col("weekly_offline_hours").cast(FloatType())) \
           .withColumn("job_satisfaction_score", col("job_satisfaction_score").cast(FloatType()))

    print("Схема преобразованных данных:")
    df.printSchema()

    # Удаление строк с пропущенными значениями
    df = df.na.drop()

    print("Пример данных после преобразования:")
    df.show(5)

    print(f"Запись в Parquet: {target_path}")
    df.write.mode("overwrite").parquet(target_path)

    print("✅ Данные успешно сохранены в Parquet.")

except AnalysisException as ae:
    print("❌ Ошибка анализа:", ae)
except Exception as e:
    print("❌ Общая ошибка:", e)

spark.stop()

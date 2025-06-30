from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder \
        .appName("Perform Analytics") \
        .getOrCreate()

    database = "test"
    host = "postgres_user"
    port = 5432
    # Параметры подключения
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    user = "user"
    password = "password"
    table_name = "sales_data"

    # Загрузка данных из PostgreSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .load()

    # Выполнение аналитических операций
    aggregate_data = df.groupBy("region", "product_id") \
        .agg(
        F.count("sale_id").alias("total_sales"),  # Общее количество продаж
        F.sum("price").alias("total_sales_amount"),  # Сумма продаж
        (F.sum("price") / F.count("sale_id")).alias("average_sale_amount")  # Средний чек
    )

    # Печать агрегированных данных для проверки
    aggregate_data.show()

    # Сохранение агрегированных данных в отдельную таблицу в PostgreSQL
    aggregate_table_name = "aggregated_sales_data"

    aggregate_data.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", aggregate_table_name) \
        .option("user", user) \
        .option("password", password) \
        .mode("overwrite").save()

    print(f"Агрегированные данные успешно сохранены в таблицу {aggregate_table_name}.")


if __name__ == '__main__':
    main()

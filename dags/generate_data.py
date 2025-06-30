from pyspark.sql import SparkSession
from pyspark.sql import Row
import random
from datetime import datetime, timedelta
import time


def generate_test_data():
    start_time = time.time()  # Запоминаем время начала выполнения

    num_sales = 1000000  # Общее количество записей
    regions = ['North', 'South', 'East', 'West']

    # Создаем SparkSession
    spark = SparkSession.builder \
        .appName("Data Generator") \
        .getOrCreate()

    # Определяем начало текущего года
    start_of_year = datetime(datetime.now().year, 1, 1)

    # Генерация валидных данных
    valid_data = []
    for sale_id in range(1, num_sales + 1):
        row = Row(
            sale_id=sale_id,
            customer_id=random.randint(1, 1000),
            product_id=random.randint(1, 500),
            quantity=random.randint(1, 10),
            price=round(random.uniform(10, 1000), 2),
            region=random.choice(regions),
            sale_date=(start_of_year + timedelta(
                days=random.randint(0, (datetime.now() - start_of_year).days))).strftime('%Y-%m-%d')
        )
        valid_data.append(row)

    # Создаем DataFrame с валидными данными
    valid_df = spark.createDataFrame(valid_data)

    # Генерация невалидных данных (0.5%)
    num_invalid = int(num_sales * 0.005)  # 0.5% невалидных данных
    invalid_data = []

    for sale_id in range(num_sales + 1, num_sales + num_invalid + 1):
        if random.random() < 0.5:
            sale_date = 'invalid'  # Неверный формат
        else:
            # Генерация даты, которая явно вне диапазона
            sale_date = (datetime.now() + timedelta(days=random.randint(1, 730))).strftime('%Y-%m-%d')  # Будущая дата

        row = Row(
            sale_id=sale_id,
            customer_id=random.randint(1, 1000),
            product_id=random.randint(1, 500),
            quantity=random.randint(1, 10),
            price=round(random.uniform(10, 1000), 2),
            region=random.choice(regions),
            sale_date=sale_date
        )
        invalid_data.append(row)

    # Создаем DataFrame с невалидными данными
    invalid_df = spark.createDataFrame(invalid_data)

    # Генерация дубликатов из существующих sale_id
    num_duplicates = int(num_sales * 0.0005)  # 0.05% дубликатов
    duplicates = []  # Список для хранения дубликатов
    for _ in range(num_duplicates):
        # Случайная выборка из валидных данных
        original_row = valid_df.sample(withReplacement=False, fraction=0.01).limit(1).collect()[0]
        duplicate_row = Row(
            sale_id=original_row.sale_id,
            customer_id=original_row.customer_id,
            product_id=original_row.product_id,
            quantity=original_row.quantity,
            price=original_row.price,
            region=original_row.region,
            sale_date=original_row.sale_date
        )

        valid_df = valid_df.union(spark.createDataFrame([duplicate_row]))
        duplicates.append(duplicate_row)  # Добавляем дубликат в список

    # Объединяем валидные и невалидные данные
    combined_df = valid_df.union(invalid_df)

    # Сохраняем в CSV
    combined_df.write.csv('sales_data.csv', header=True, mode='overwrite')
    print(f"Сгенерировано {combined_df.count()} записей. Сохранено в sales_data.csv")

    end_time = time.time()  # Запоминаем время окончания выполнения
    execution_time = end_time - start_time  # Вычисляем время выполнения
    print(f"Время выполнения: {execution_time:.2f} секунд")


if __name__ == '__main__':
    generate_test_data()

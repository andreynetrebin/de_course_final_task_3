from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
import json


class DataCleaner:
    def __init__(self, filepath, invalid_ids):
        self.spark = SparkSession.builder \
            .appName("Data Cleaner") \
            .getOrCreate()
        self.df = self.spark.read.csv(filepath, header=True, inferSchema=True)
        self.invalid_ids = invalid_ids

    def clean_data(self):
        # Удаляем дубликаты по столбцу sale_id
        initial_row_count = self.df.count()
        self.df = self.df.dropDuplicates(["sale_id"])
        print(f"Удалено дубликатов: {initial_row_count - self.df.count()}")

        # Сохраняем плохие данные
        bad_data_ids = set(self.invalid_ids)
        bad_data_df = self.df.filter(col("sale_id").isin(bad_data_ids))

        bad_data_df.write.csv('bad_sales_data.csv', header=True, mode='overwrite')
        print(f"Плохие данные сохранены в 'bad_sales_data.csv'. Всего записей: {bad_data_df.count()}")

        # Удаляем плохие данные
        cleaned_df = self.df.filter(~col("sale_id").isin(bad_data_ids))

        cleaned_df.write.csv('cleaned_sales_data.csv', header=True, mode='overwrite')
        print(f"Очищенные данные сохранены в 'cleaned_sales_data.csv'. Всего записей: {cleaned_df.count()}")


if __name__ == '__main__':
    start_time = time.time()  # Запоминаем время начала выполнения

    # Загружаем список недопустимых ID из отчета
    with open('data_validation_report.json', 'r') as f:
        report = json.load(f)
        invalid_ids = set(
            report['duplicate_ids'] + report['invalid_dates'] + report['invalid_regions'] + report['invalid_numeric'])

    # Создаем очиститель и очищаем данные
    cleaner = DataCleaner('sales_data.csv', invalid_ids)
    cleaner.clean_data()

    end_time = time.time()  # Запоминаем время окончания выполнения
    execution_time = end_time - start_time  # Вычисляем время выполнения
    print(f"\nВремя выполнения: {execution_time:.3f} секунд")

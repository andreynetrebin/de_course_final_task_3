from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from datetime import datetime
import time
import json


class DataValidator:
    def __init__(self, filepath):
        self.spark = SparkSession.builder \
            .appName("Data Validator") \
            .getOrCreate()
        self.df = self.spark.read.csv(filepath, header=True, inferSchema=True)
        self.current_year = datetime.now().year
        self.issues = []
        self.valid_regions = ['North', 'South', 'East', 'West']

    def check_duplicates(self):
        duplicate_rows = self.df.groupBy("sale_id").agg(count("*").alias("count")).filter(col("count") > 1)
        duplicate_count = duplicate_rows.count()
        if duplicate_count > 0:
            self.issues.append(f"Найдены дубликаты ID: {duplicate_count} записей")
            duplicate_rows.write.csv('duplicate_sales_data.csv', header=True, mode='overwrite')
            return [row.sale_id for row in duplicate_rows.collect()]
        return []

    def check_dates(self):
        # Проверка на неверный формат даты
        invalid_dates = self.df.filter(~self.df.sale_date.rlike(r'^\d{4}-\d{2}-\d{2}')).select("sale_id").distinct()
        invalid_date_count = invalid_dates.count()
        if invalid_date_count > 0:
            self.issues.append(f"Неверный формат даты: {invalid_date_count} записей")
            invalid_ids = [row.sale_id for row in invalid_dates.collect()]
        else:
            invalid_ids = []

        # Проверка на диапазон дат
        start_date = datetime(self.current_year, 1, 1).date()
        end_date = datetime.now().date()

        # Исправлено условие для проверки дат вне диапазона
        out_of_range_dates = self.df.filter(
            (col("sale_date") < start_date.strftime('%Y-%m-%d')) |
            (col("sale_date") > end_date.strftime('%Y-%m-%d'))
        ).select("sale_id").distinct()

        out_of_range_count = out_of_range_dates.count()
        if out_of_range_count > 0:
            self.issues.append(f"Дата вне диапазона: {out_of_range_count} записей")
            invalid_ids.extend([row.sale_id for row in out_of_range_dates.collect()])

        return list(set(invalid_ids))  # Удаляем дубликаты из списка проблемных ID

    def check_regions(self):
        invalid_regions = self.df.filter(~self.df.region.isin(self.valid_regions)).select("sale_id").distinct()
        invalid_region_count = invalid_regions.count()
        if invalid_region_count > 0:
            self.issues.append(f"Неверные регионы: {invalid_region_count} записей")
            return [row.sale_id for row in invalid_regions.collect()]
        return []

    def check_numeric_values(self):
        invalid_qty = self.df.filter(~(col("quantity").between(1, 10))).select("sale_id").distinct()
        invalid_price = self.df.filter(~(col("price").between(0.01, 1000))).select("sale_id").distinct()

        issues = []
        if invalid_qty.count() > 0:
            issues.append(f"Некорректное количество: {invalid_qty.count()} записей")
        if invalid_price.count() > 0:
            issues.append(f"Некорректная цена: {invalid_price.count()} записей")

        if issues:
            self.issues.extend(issues)
            return invalid_qty.union(invalid_price).select("sale_id").distinct().collect()
        return []

    def generate_report(self):
        report = {
            'total_rows': self.df.count(),
            'duplicate_ids': self.check_duplicates(),
            'invalid_dates': self.check_dates(),
            'invalid_regions': self.check_regions(),
            'invalid_numeric': self.check_numeric_values(),
            'issues': self.issues,
            'valid_percentage': None
        }

        # Объединяем все проблемные ID
        all_invalid_ids = set(
            report['duplicate_ids'] + report['invalid_dates'] + report['invalid_regions'] + report['invalid_numeric'])

        valid_rows = report['total_rows'] - len(all_invalid_ids)
        report['valid_percentage'] = (valid_rows / report['total_rows']) * 100

        # Сохранение отчета в файл
        with open('data_validation_report.json', 'w') as f:
            json.dump(report, f, ensure_ascii=False, indent=4)

        return report, all_invalid_ids  # Возвращаем и отчет, и все проблемные ID


if __name__ == '__main__':
    start_time = time.time()  # Запоминаем время начала выполнения

    # Создаем валидатор и проверяем данные
    validator = DataValidator('sales_data.csv')
    report, invalid_ids = validator.generate_report()  # Получаем и отчет, и список недопустимых ID

    # Выводим отчет
    print("\n=== Отчет о проверке данных ===")
    print(f"Всего записей: {report['total_rows']}")
    print(f"Процент валидных данных: {report['valid_percentage']:.2f}%")

    print("\nНайденные проблемы:")
    for issue in report['issues']:
        print(f" - {issue}")

    print("\nПримеры проблемных ID:")
    print(f"Дубликаты: {report['duplicate_ids'][:3]}...")
    print(f"Неверные даты: {report['invalid_dates'][:3]}...")
    print(f"Неверные регионы: {report['invalid_regions'][:3]}...")
    print(f"Некорректные значения: {report['invalid_numeric'][:3]}...")

    end_time = time.time()  # Запоминаем время окончания выполнения
    execution_time = end_time - start_time  # Вычисляем время выполнения
    print(f"\nВремя выполнения: {execution_time:.2f} секунд")

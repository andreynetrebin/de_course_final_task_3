from pyspark.sql import SparkSession
from clickhouse_driver import Client

def create_table_if_not_exists():
    # Подключение к ClickHouse
    client = Client(host='clickhouse_user', port=9000)

    # SQL-запрос для создания таблицы
    create_table_query = """
    CREATE TABLE IF NOT EXISTS aggregated_sales_data (
        region String,
        product_id Int32,
        total_sales Int32,
        total_sales_amount Float64,
        average_sale_amount Float64,
        import_date DateTime
    ) ENGINE = MergeTree()
    ORDER BY region;
    """

    # Выполняем запрос на создание таблицы
    client.execute(create_table_query)
    print("Таблица aggregated_sales_data успешно создана.")

def transfer_data():
    spark = SparkSession.builder \
        .appName("Transfer Data to ClickHouse") \
        .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc-0.3.2.jar").getOrCreate()

    # Подключение к PostgreSQL
    database = "test"
    host = "postgres_user"
    port = 5432
    jdbc_url_pg = f"jdbc:postgresql://{host}:{port}/{database}"
    pg_user = "user"
    pg_password = "password"
    pg_table = "aggregated_sales_data"

    # Загрузка данных из PostgreSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url_pg) \
        .option("dbtable", pg_table) \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .load()
    print("Данные успешно прочитаны из PostgreSQL.")

    # Добавление даты импорта
    from pyspark.sql.functions import current_timestamp
    df_with_date = df.withColumn("import_date", current_timestamp())

    # Создаем таблицу в ClickHouse, если она не существует
    create_table_if_not_exists()

    # Подключение к ClickHouse
    client = Client(host='clickhouse_user', port=9000)

    # Преобразуем DataFrame в список словарей для вставки в ClickHouse
    records = [{**row.asDict()} for row in df_with_date.collect()]

    # Сохранение данных в ClickHouse
    client.execute('INSERT INTO aggregated_sales_data VALUES', records)
    print("Данные успешно перенесены в ClickHouse.")

if __name__ == '__main__':
    transfer_data()

from pyspark.sql import SparkSession
import psycopg2


def create_table_if_not_exists():
    # Параметры подключения к PostgreSQL
    database = "test"
    host = "postgres_user"
    port = 5432
    user = "user"
    password = "password"

    # Создаем соединение с PostgreSQL
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()

    # SQL-запрос для создания таблицы
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales_data (
        sale_id SERIAL PRIMARY KEY,
        customer_id INT,
        product_id INT,
        quantity INT,
        price DECIMAL(10, 2),
        region VARCHAR(50),
        sale_date DATE
    );
    """

    # Выполняем запрос на создание таблицы
    cursor.execute(create_table_query)
    conn.commit()

    # Закрываем соединение
    cursor.close()
    conn.close()
    print("Таблица sales_data успешно создана.")


def load_data_to_postgresql():
    # Путь к JAR-файлу драйвера PostgreSQL
    jdbc_driver_path = "/opt/spark/jars/postgresql-42.7.7.jar"
    # Создаем SparkSession с указанием JAR-файла
    spark = SparkSession.builder \
        .appName("Load Data to PostgreSQL") \
        .config("spark.jars", jdbc_driver_path) \
        .getOrCreate()

    # Загрузка очищенных данных в DataFrame
    cleaned_data_df = spark.read.csv("cleaned_sales_data.csv", header=True, inferSchema=True)

    # Проверка схемы
    cleaned_data_df.printSchema()

    database = "test"
    host = "postgres_user"
    port = 5432
    # Параметры подключения
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    user = "user"
    password = "password"
    table_name = "sales_data"

    # Создаем таблицу, если она не существует
    create_table_if_not_exists()

    # Загружаем данные в PostgreSQL
    cleaned_data_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite").save()

    print(f"Данные успешно загружены в таблицу {table_name}.")


if __name__ == '__main__':
    load_data_to_postgresql()

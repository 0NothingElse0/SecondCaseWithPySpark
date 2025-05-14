from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

try: 
    spark = SparkSession.builder.appName("ProductsWithCategories") .getOrCreate()

    #Определяем схемы данных для фреймов
    products_schema = StructType([
        StructField("productId", IntegerType(), False),
        StructField("productName", StringType(), False)
    ])

    categories_schema = StructType([
        StructField("categoryId", IntegerType(), False),
        StructField("categoryName", StringType(), False)
    ])

    links_schema = StructType([
        StructField("productId", IntegerType(), False),
        StructField("categoryId", IntegerType(), False)
    ])

    #Определяем данные для фреймов
    products_data = [
        (1, "First product"),
        (2, "Second product"),
        (3, "Third product"),
        (4, "Fourth product without category")
    ]

    categories_data = [
        (1, "First category"),
        (2, "Second category"),
        (3, "Third category")
    ]

    links_data = [
        (1, 1),
        (1, 2),
        (2, 2),
        (3, 1)
    ]

    # Создаем DataFrame для продуктов, категорий и связи таблиц
    products = spark.createDataFrame(products_data, products_schema)
    categories = spark.createDataFrame(categories_data, categories_schema)
    product_categories = spark.createDataFrame(links_data, links_schema)

    # Создаем временные представления для дальнейшей работы
    products.createOrReplaceTempView("products")
    products.show()
    categories.createOrReplaceTempView("categories")
    categories.show()
    product_categories.createOrReplaceTempView("product_categories")
    product_categories.show()

    # SQL запросом получаем необходимые данные
    result = spark.sql("""
        SELECT p.productName, c.categoryName
        FROM products p
        LEFT JOIN product_categories pc ON p.productId = pc.productId
        LEFT JOIN categories c ON pc.categoryId = c.categoryId
    """)

    result.show()
except Exception as e:
    print(f"Error occurred: {str(e)}")
finally:
    spark.stop()
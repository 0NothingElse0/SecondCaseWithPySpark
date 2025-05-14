# Задание:

1. 
В PySpark приложении датафреймами(pyspark.sql.DataFrame) заданы продукты, категории и их связи. Каждому продукту может соответствовать несколько категорий или ни одной. А каждой категории может соответствовать несколько продуктов или ни одного. Напишите метод на PySpark, который в одном датафрейме вернет все пары «Имя продукта – Имя категории» и имена всех продуктов, у которых нет категорий.

# Результат работы программы
PySpark запускал в контейнере Docker jupyter/all-spark-notebook
+---------+--------------------+
|productId|         productName|
+---------+--------------------+
|        1|       First product|
|        2|      Second product|
|        3|       Third product|
|        4|Fourth product wi...|
+---------+--------------------+

+----------+---------------+
|categoryId|   categoryName|
+----------+---------------+
|         1| First category|
|         2|Second category|
|         3| Third category|
+----------+---------------+

+---------+----------+
|productId|categoryId|
+---------+----------+
|        1|         1|
|        1|         2|
|        2|         2|
|        3|         1|
+---------+----------+

+--------------------+---------------+
|         productName|   categoryName|
+--------------------+---------------+
|Fourth product wi...|           NULL|
|       First product| First category|
|       Third product| First category|
|       First product|Second category|
|      Second product|Second category|
+--------------------+---------------+
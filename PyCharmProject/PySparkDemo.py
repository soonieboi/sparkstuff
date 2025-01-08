from pyspark.sql import SparkSession
filename="dummy.txt"
spark = SparkSession.builder.appName('Basics').getOrCreate()
print(spark.read.text(filename).count())

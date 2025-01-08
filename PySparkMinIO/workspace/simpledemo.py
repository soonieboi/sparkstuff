from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local") \
              .appName("Ingestion") \
              .cspraonfig('spark.ui.port', '4050') \
              .getOrCreate()
# Read CSV file people.csv
df = spark.read.format('csv') \
                .option("inferSchema","true") \
                .option("header","true") \
                .option("sep",";") \
                .load("/workspace/movies.csv")

# Show result
df.show()
# Print schema
df.printSchema()
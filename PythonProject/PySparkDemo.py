from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, lower
filename = "dummy.txt"
# the above file is under your pythonProject folder
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
# print(spark.read.text(filename).count())

df = spark.read.text("dummy.txt")
# Count total words

# word_count = df.select(explode(split(df.value, r"\W+")).alias("word")) \
#                .filter("word != ''") \
#                .count()
# print("Total words:", word_count)

# Most common words
top_words = df.select(explode(split(lower(df.value), r"\W+")).alias("word")) \
              .filter("word != ''") \
              .groupBy("word") \
              .count() \
              .orderBy("count", ascending=False)

top_words.show(10)
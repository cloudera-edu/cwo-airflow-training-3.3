from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('WordCount').getOrCreate()

# Create sample data
data = [('Hello World',), ('Hello Airflow',), ('CDE Airflow Training',)]
df = spark.createDataFrame(data, ['text'])

# Word count
from pyspark.sql.functions import explode, split, col
words = df.select(explode(split(col('text'), ' ')).alias('word'))
word_counts = words.groupBy('word').count().orderBy('count', ascending=False)

word_counts.show()
print(f'Total unique words: {word_counts.count()}')

spark.stop()

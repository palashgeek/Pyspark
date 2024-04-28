from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Create a SparkSession
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Read the input text file
input_file_path = r"C:\Users\Lenovo\PYTHON\Lytx-CFP-DataEngg-AWS\firstprogram.txt"
text_df = spark.read.text(input_file_path)

# Tokenize the text into individual words
words_df = text_df.select(explode(split(col("value"), "\\s+")).alias("word"))

# Count the occurrences of each word
word_counts_df = words_df.groupBy("word").count()

# Display the word counts
word_counts_df.show()

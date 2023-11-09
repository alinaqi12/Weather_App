from pyspark import SparkContext
from pyspark.sql import SparkSession

# Initialize SparkContext and SparkSession
sc = SparkContext("local", "PySpark Example")
spark = SparkSession(sc)

# Load data into a DataFrame
df = spark.read.csv("test2.csv", header=True, inferSchema=True)

# Show the first few rows
df.show()

# Select specific columns
selected_df = df.select("Name", "age")

# Show the selected DataFrame
selected_df.show()

# Filter data
filtered_df = df.filter(df["age"] > 10)

# Stop SparkSession
spark.stop()
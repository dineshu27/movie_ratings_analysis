from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, row_number

# Create Spark Session
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()

# Read movies.dat and ratings.dat to Spark DataFrames
movies_df = spark.read.option("delimiter", "::").csv("/mnt/codepath/movies.dat", header=False, inferSchema=True).toDF("movieId", "title", "genres")
ratings_df = spark.read.option("delimiter", "::").csv("/mnt/codepath/ratings.dat", header=False, inferSchema=True).toDF("userId", "movieId", "rating", "timestamp")

# Create a new DataFrame with max, min, and average ratings for each movie
movie_ratings_df = ratings_df.groupBy("movieId").agg(
    max("rating").alias("max_rating"),
    min("rating").alias("min_rating"),
    avg("rating").alias("avg_rating")
)
movies_with_ratings_df = movies_df.join(movie_ratings_df, "movieId")

# Create a window specification to rank movies within each user based on rating
window_spec = Window.partitionBy("userId").orderBy(col("rating").desc())

# Add a rank column to the joined DataFrame based on the descending order of ratings
user_top_movies_df = ratings_df.join(movies_df, "movieId").select("userId", "movieId", "title", "rating").withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 3).drop("rank")

# Write out the New DataFrames in Parquet format
user_top_movies_df.write.parquet("/mnt/codepath/user_top_movies.parquet", mode="overwrite")
movies_with_ratings_df.write.parquet("/mnt/codepath/movies_with_ratings.parquet", mode="overwrite")

# Read Output Parquet files and show values
user_top_movies_df_read = spark.read.parquet("/mnt/codepath/user_top_movies.parquet")
movies_with_ratings_df_read = spark.read.parquet("/mnt/codepath/movies_with_ratings.parquet")

print("User Top Movies DataFrame:")
user_top_movies_df_read.show()

print("Movies with Ratings DataFrame:")
movies_with_ratings_df_read.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, BooleanType
from pathlib import Path
import pandas as pd

spark = SparkSession.builder \
    .appName("BookETL") \
    .getOrCreate()

@udf(StringType())
def fix_mojibake_udf(s):
    try:
        return s.encode("latin1").decode("utf-8")
    except:
        return s

@udf(BooleanType())
def is_valid_isbn_udf(isbn):
    try:
        total = 0
        for idx, char in enumerate(isbn):
            val = 10 if char == 'X' else int(char)
            total += val * (10 - idx)
        return total % 11 == 0
    except:
        return False


def load_and_clean_data_with_spark(from_scratch=False):
    datasets_dir = Path(__file__).parent / "datasets"
    books_path = str(datasets_dir / "Books.csv")
    ratings_path = str(datasets_dir / "Ratings.csv")
    output_path = str(datasets_dir / "dataset.pkl")

    if Path(output_path).exists() and not from_scratch:
        return pd.read_pickle(output_path)

    # Load CSVs
    books_df = spark.read.csv(books_path, header=True, inferSchema=True)
    ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)

    # Clean ratings
    ratings_df = ratings_df.dropDuplicates(["ISBN", "User-ID"])
    ratings_df = ratings_df.filter((ratings_df["Book-Rating"] > 0) & (ratings_df["Book-Rating"] <= 10))
    ratings_df = ratings_df.filter(ratings_df["ISBN"].rlike(r"^[0-9X]+$"))
    ratings_df = ratings_df.filter(is_valid_isbn_udf("ISBN"))

    # Clean books
    books_df = books_df.dropna(subset=["Book-Title", "Book-Author", "ISBN"])
    books_df = books_df.drop("Publisher", "Year-Of-Publication", "Image-URL-S", "Image-URL-M", "Image-URL-L")
    books_df = books_df.withColumn("Book-Title", fix_mojibake_udf("Book-Title"))
    books_df = books_df.withColumn("Book-Author", fix_mojibake_udf("Book-Author"))
    books_df = books_df.dropDuplicates(["Book-Title"])
    books_df = books_df.filter(books_df["ISBN"].rlike(r"^[0-9X]+$"))
    books_df = books_df.filter(is_valid_isbn_udf("ISBN"))

    # Join
    full_df = ratings_df.join(books_df, on="ISBN", how="inner")

    # Collect as pandas and save
    full_pd_df = full_df.toPandas()
    full_pd_df.to_pickle(output_path)

    return full_pd_df
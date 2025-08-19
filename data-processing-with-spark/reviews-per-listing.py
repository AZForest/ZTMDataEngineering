import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

parser = argparse.ArgumentParser(description="Most popular listings parameters")
parser.add_argument('--listings', help='Path to the listings dataset')
parser.add_argument('--reviews', help='Path to the reviews dataset')
parser.add_argument('--output', help='Directory to the save output')
args = parser.parse_args()

spark = SparkSession.builder \
    .appName("Spark aggregation functions") \
    .getOrCreate()

# listings = spark.read.csv("./data/listings.csv.gz", 
listings = spark.read.csv(args.listings,
    header=True,
    inferSchema=True,
    sep=",", 
    quote='"',
    escape='"', 
    multiLine=True,
    mode="PERMISSIVE" 
)

# reviews = spark.read.csv("./data/reviews.csv.gz", 
reviews = spark.read.csv(args.reviews,
    header=True,
    inferSchema=True,
    sep=",",
    quote='"',
    escape='"',
    multiLine=True,
    mode="PERMISSIVE"
)

listings_reviews = listings.join(
    reviews, listings.id == reviews.listing_id, how="inner"
)

reviews_per_listing = listings_reviews \
    .groupBy(listings.id, listings.name) \
    .agg(
        F.count(reviews.id).alias('num_reviews')
    ) \
    .orderBy('num_reviews', ascending=False) \

reviews_per_listing \
    .write \
    .csv(args.output)
    # .csv('data/output')

# Example cmd from cmd-line
# PYSPARK_DRIVER_PYTHON=./venv/bin/python PYSPARK_PYTHON=./venv/bin/python spark-submit reviews-per-listing.py --listings data/listings.csv.gz --reviews data/reviews.csv.gz --output data/output_2

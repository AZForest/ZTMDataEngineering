import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--customer_reviews", required=True, help="Input CSV file path")
    parser.add_argument("--output_path", required=True, help="Output CSV file path")
    args = parser.parse_args()

    spark = SparkSession \
        .builder \
        .appName("CustomerReviews") \
        .getOrCreate()

    # TODO: Read input data
    customer_reviews = spark.read.csv(args.customer_reviews,
        header=True,
        inferSchema=True,
        sep=",",
        quote='"',
        escape='"',
        multiLine=True,
        mode="PERMISSIVE"
    )

    customer_reviews = customer_reviews \
        .withColumn("review_score", customer_reviews["review_score"].cast("float"))

    # TODO: Calculate an average review score per listing ID
    aggregated = customer_reviews \
        .groupBy(customer_reviews.listing_id) \
        .agg(
            avg(customer_reviews.review_score).alias("avg_review_score"),
            count(customer_reviews.review_id).alias("num_reviews")
        )
    # TODO: Write the result to an output path
    aggregated.write.mode("overwrite").csv(args.output_path)
    print(f"Aggregated results written to {args.output_path}")
    spark.stop()

if __name__ == "__main__":
    main()
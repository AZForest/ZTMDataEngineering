from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Hello World from Spark!")

    spark = SparkSession.builder.appName("HelloWorldTest").getOrCreate()
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    df.show()
    spark.stop()
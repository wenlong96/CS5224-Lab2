BUCKET = "cs5224-lab2-twitter-a0262582b"
INPUT_PATH = f"s3://{BUCKET}/input/twitter_combined.txt"
OUTPUT_PATH = f"s3://{BUCKET}/output/user_counts/"
PARTITIONS = 8


def parse_edge(line: str):
    """Ignore blank lines; reject malformed rows rather than silently losing data."""
    parts = line.split()
    if not parts:
        return []
    if len(parts) != 2 or not all(p.isascii() and p.isdecimal() for p in parts):
        raise ValueError(f"Expected two numeric user IDs, got: {line!r}")
    return [(parts[0], parts[1])]


def map_edge(edge):
    """Emit each user's contribution as (user_id, (followers, followees))."""
    source, target = edge
    return [(source, (0, 1)), (target, (1, 0))]


def add_counts(a, b):
    """Reduce by user ID: add follower counts and followee counts separately."""
    return (a[0] + b[0], a[1] + b[1])


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("CS5224-Lab2-Twitter-Counts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read the complete input file and deduplicate ordered relationships.
        edges = (
            spark.sparkContext.textFile(INPUT_PATH, minPartitions=PARTITIONS)
            .flatMap(parse_edge)
            .distinct(numPartitions=PARTITIONS)
        )

        # Map and reduce in parallel. Emitting both endpoints preserves zero counts.
        counts = edges.flatMap(map_edge).reduceByKey(
            add_counts, numPartitions=PARTITIONS
        )

        # Turn each result into a named record for the backend to read later.
        rows = counts.map(lambda item: (item[0], item[1][0], item[1][1]))
        results = spark.createDataFrame(
            rows, "user_id string, followers long, followees long"
        ).cache()

        # Print evidence in the Spark driver log
        print(f"Input: {INPUT_PATH}")
        print(f"Total users: {results.count()}")
        results.filter(
            results.user_id.isin("214328887", "107830991")
        ).orderBy("user_id").show(truncate=False)

        # Save all users
        results.write.mode("overwrite").json(OUTPUT_PATH)
        print(f"Results written to: {OUTPUT_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

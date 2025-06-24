from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def compute_hits(records):
    hits = 0
    none_count = 0
    last_color = None

    for _, state, ttype in records:
        if state == 'none':
            none_count += 1
            if none_count >= 3:
                last_color = None
            continue

        none_count = 0

        if state == 'entire':
            last_color = ttype.split('_')[0]

        elif state == 'broken':
            color = ttype.split('_')[0]
            if last_color == color:
                hits += 1
            # не даём двойного срабатывания
            last_color = None

    return hits

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PlateDetectHitsRDD").getOrCreate()

    df = spark.read.csv("results_all.csv", header=True, inferSchema=True)
    first_col = df.columns[0]
    df = df.withColumnRenamed(first_col, "video_filename")

    rdd = (
        df.select("video_filename", "timestamp", "state", "target_type").rdd
          .map(lambda r: (r.video_filename, (r.timestamp, r.state, r.target_type)))
          .groupByKey()
          .mapValues(lambda recs: compute_hits(sorted(recs, key=lambda x: x[0])))
    )

    schema = StructType([
        StructField("video_filename", StringType(), True),
        StructField("hits", IntegerType(), True),])
    result = spark.createDataFrame(rdd, schema)

    result.show(truncate=False)
    result.coalesce(1).write.csv("hits_per_video.csv", header=True, mode="overwrite")

    spark.stop()

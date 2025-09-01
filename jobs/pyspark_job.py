import argparse
from pyspark.sql import SparkSession, functions as F

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--gcs_output", required=True)
    parser.add_argument("--bq_dataset", required=True)
    parser.add_argument("--project_id", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("curso-proyecto-final")
        .config("viewsEnabled", "true")
        .config("materializationDataset", args.bq_dataset)
        .getOrCreate()
    )

    df = spark.read.option("header", True).option("inferSchema", True).csv(args.input)

    str_cols = ["experience_level", "employment_type", "job_title", "employee_residence", "company_location", "company_size"]
    for c in str_cols:
        if c in df.columns:
            df = df.withColumn(c, F.lower(F.trim(F.col(c))))

    required_cols = ["experience_level", "job_title", "salary_in_usd", "company_location"]
    df = df.dropna(subset=[c for c in required_cols if c in df.columns])

    if "salary_in_usd" in df.columns:
        df = df.withColumn("salary_in_usd", F.col("salary_in_usd").cast("double"))

    clean_output = f"{args.gcs_output}clean/"
    df.write.mode("overwrite").option("header", True).csv(clean_output)

    base_table = f"{args.project_id}.{args.bq_dataset}.salaries_clean"
    df.write.format("bigquery").option("table", base_table).mode("overwrite").save()

    by_exp = df.groupBy("experience_level").agg(F.avg("salary_in_usd").alias("AVG_USD"))
    by_exp.write.format("bigquery").option("table", f"{args.project_id}.{args.bq_dataset}.salaries_by_experience").mode("overwrite").save()

    by_job = df.groupBy("job_title").agg(F.avg("salary_in_usd").alias("AVG_USD"))
    by_job.write.format("bigquery").option("table", f"{args.project_id}.{args.bq_dataset}.salaries_by_job").mode("overwrite").save()

    job_stats = df.groupBy("job_title").agg(F.max("salary_in_usd").alias("max_salary_usd"), F.min("salary_in_usd").alias("min_salary_usd"))
    max_row = job_stats.orderBy(F.col("max_salary_usd").desc()).limit(1).withColumn("metric", F.lit(1))
    min_row = job_stats.orderBy(F.col("min_salary_usd").asc()).limit(1).withColumn("metric", F.lit(0))
    extremes = max_row.unionByName(min_row)
    extremes.write.format("bigquery").option("table", f"{args.project_id}.{args.bq_dataset}.job_salary_extremes").mode("overwrite").save()

    select_cols = [c for c in ["job_title", "experience_level", "company_location", "salary_in_usd"] if c in df.columns]
    top10 = df.select(*select_cols).orderBy(F.col("salary_in_usd").desc()).limit(10)
    top10.write.format("bigquery").option("table", f"{args.project_id}.{args.bq_dataset}.top10_salaries").mode("overwrite").save()

    spark.stop()

if __name__ == "__main__":
    main()

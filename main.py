from pyspark.sql import SparkSession

from transform.utils import Transform, logger, save_dataframe

spark = SparkSession.builder.appName("hema-spark").getOrCreate()

FILE_DIRECTORY = "./data"
FILENAME = "train.csv"


def handler():
    # read file
    ingested_df = (
        spark.read.format("csv")
        .option("header", True)
        .load(f"{FILE_DIRECTORY}/{FILENAME}")
    )

    transformer = Transform(filename=FILENAME)

    raw = transformer.ingestion_to_raw(ingested_df)
    curated = transformer.raw_to_curated_pipeline(raw)
    sales = transformer.curated_to_sales(curated)
    customers = transformer.curated_to_customers(curated)

    logger.debug("Saving raw dataframe...")
    save_dataframe(
        df=raw,
        location="./output/raw",
        format="csv",
        mode="append",
        partition_col="Order Date",
    )
    logger.debug("OK!")

    logger.debug("Saving curated dataframe...")
    save_dataframe(
        df=curated,
        location="./output/curated",
        format="parquet",
        mode="append",
        partition_col="orderDate",
    )
    logger.debug("OK!")

    logger.debug("Saving sales dataframe...")
    save_dataframe(
        df=sales,
        location="./output/sales",
        format="parquet",
        mode="append",
        partition_col="orderDate",
    )
    logger.debug("OK!")

    logger.debug("Saving sales dataframe...")
    save_dataframe(
        df=customers,
        location="./output/customers",
        format="parquet",
        mode="overwrite",
    )
    logger.debug("OK!")


if __name__ == "__main__":
    handler()

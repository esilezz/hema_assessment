import logging
from datetime import datetime, timedelta
from re import sub
from sys import stdout

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    current_date,
    current_timestamp,
    date_format,
    lit,
    split,
    to_date,
    when,
)

from .schema import dtypes

logger = logging.getLogger("hema-assessment")

logger.setLevel(logging.DEBUG)  # set logger level
logFormatter = logging.Formatter(
    "%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s"
)
consoleHandler = logging.StreamHandler(stdout)  # set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

augmented_cols = ["filename", "ingestionDate", "time"]
date_columns = ["orderDate", "shipDate"]


def camel_case(s: str) -> str:
    """
    Convert a string from Title Case to camelCase
    """
    s = sub(r"(_|-)+", " ", s).title().replace(" ", "")
    return "".join([s[0].lower(), s[1:]])


def save_dataframe(
    df: DataFrame, location: str, format: str, mode: str, partition_col: str = None
) -> None:
    if format == "csv":
        (
            df.write.option("header", True)
            .partitionBy(partition_col)
            .mode(mode)
            .csv(location)
        )
    elif format == "parquet":
        if partition_col:
            (
                df.write.option("header", True)
                .partitionBy(partition_col)
                .mode(mode)
                .parquet(location)
            )
        else:
            df.write.option("header", True).mode(mode).parquet(location)
    else:
        logger.error(
            f"ERROR - the output format specified (<{format}>)"
            "is not valid. Valid choices are <csv> or <parquet>."
        )


class Transform:
    def __init__(self, filename: str) -> None:
        self.filename = filename

    @staticmethod
    def augment_dataframe(df: DataFrame, filename: str) -> DataFrame:
        """
        Augment the dataframe with metadata columns like `filename`,
        ingestion date and time. If the said columns are already present,
        drop them and update them with new values

        Args:
            df (DataFrame): dataframe to augment
            filename (str): name of the original file
        """

        if augmented_cols in df.columns:
            df = df.drop(("filename", "ingestionDate", "time"))

        df = (
            df.withColumn("filename", lit(filename))
            .withColumn("ingestionDate", current_date())
            .withColumn("curr_timestamp", current_timestamp())
            .withColumn("time", date_format("curr_timestamp", "HH:mm:ss"))
            .drop(col("curr_timestamp"))
        )
        return df

    def ingestion_to_raw(self, df: DataFrame) -> DataFrame:
        """
        Function to go from the original ingested file to the raw version
        """
        logger.debug("Processing from original to raw format...")
        raw_df = self.augment_dataframe(df, self.filename)
        logger.debug("Done!")
        return raw_df

    def raw_to_curated_pipeline(self, df: DataFrame) -> DataFrame:
        """
        Function to go from raw to curated format. It includes:
            - column names conversion to camelCase
            - casting of columns to correct types
            - metadata augmentation
        """

        logger.debug("Processing from raw to curated format...")

        # prepare the dict for column name conversion
        camelcase_column_mapping = {col: camel_case(col) for col in df.columns}

        # rename columns and fix types
        for col_name in camelcase_column_mapping:
            logger.debug(f"Working on column <{col_name}>...")
            if col_name in augmented_cols:
                continue

            new_name = camelcase_column_mapping[col_name]
            if new_name in date_columns:
                df = df.withColumnRenamed(col_name, new_name)
                df = df.withColumn(new_name, to_date(col(new_name), "dd/MM/yyyy"))
            else:
                dtype = dtypes[new_name]
                df = df.withColumnRenamed(col_name, new_name)
                df = df.withColumn(new_name, col(new_name).cast(dtype))

        # add metadata to dataframe
        curated_df = self.augment_dataframe(df, self.filename)
        logger.debug("Done!")
        return curated_df

    def curated_to_sales(self, df: DataFrame) -> DataFrame:
        """
        Function to go from curated version to consumption format (sales)
        """
        logger.debug("Processing from curated to sales...")
        sales = df.select(
            col("orderId"),
            col("orderDate"),
            col("shipDate"),
            col("shipMode"),
            col("city"),
        )
        sales = self.augment_dataframe(sales, self.filename)
        logger.debug("Done!")
        return sales

    def curated_to_customers(self, df: DataFrame) -> DataFrame:
        """
        Function to go from curated version to consumption format (customers)
        """

        logger.debug("Processing from curated to customers...")

        logger.debug("Extracting the cols...")
        # select the columns from curated format which do not require aggregations
        customers_info = (
            df.select(
                col("customerId"),
                col("customerName"),
                col("country"),
                col("city"),
                col("segment"),
            )
            .withColumn("customerFirstName", split(col("customerName"), " ").getItem(0))
            .withColumn("customerLastName", split(col("customerName"), " ").getItem(1))
            .withColumnRenamed("segment", "customerSegment")
        )

        logger.debug("Computing the aggregations...")
        # calculate the cutoff dates for 5, 15, and 30 days ago
        cutoff_5d = datetime.now() - timedelta(days=5)
        cutoff_15d = datetime.now() - timedelta(days=15)
        cutoff_30d = datetime.now() - timedelta(days=30)

        # group the DataFrame by `customerName` and calculate the total
        # quantity of orders in the last 5, 15 and 30 days, and the total
        customers_orders = df.groupBy("customerName").agg(
            count(when(col("orderDate") >= cutoff_5d, 1)).alias(
                "quantityOfOrdersLast5Days"
            ),
            count(when(col("orderDate") >= cutoff_15d, 1)).alias(
                "quantityOfOrdersLast15Days"
            ),
            count(when(col("orderDate") >= cutoff_30d, 1)).alias(
                "quantityOfOrdersLast30Days"
            ),
            count(col("orderDate")).alias("TotalQuantityOfOrder"),
        )

        # join the two dataframes with general info and quantity of orders
        customers = (
            customers_info.join(
                customers_orders,
                customers_info.customerName == customers_orders.customerName,
            )
            .drop("customerName")
            .withColumn(
                "customerName",
                concat_ws(" ", col("customerFirstName"), col("customerLastName")),
            )
        )

        # augment the dataframe
        customers = self.augment_dataframe(customers, self.filename)
        logger.debug("Done!")
        return customers

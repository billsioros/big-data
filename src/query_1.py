import argparse
from logging import Logger

from helpers import get_logger, read_crime_data, spark, timer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, dense_rank, month, year
from pyspark.sql.window import Window


def use_sql(session: SparkSession, df: DataFrame, logger: Logger) -> DataFrame:
    """Executes a SQL query on the given DataFrame to get the top 3 months for each year with the highest number of
    crimes.

    Args:
        session (SparkSession): The SparkSession object.
        df (DataFrame): The DataFrame containing the crime data.
        logger (Logger): The logger object.

    Returns:
        DataFrame: The results of the SQL query.
    """

    logger.info("Register the DataFrame as a temporary SQL table.")
    df.createOrReplaceTempView("crime_data")

    query = """
    SELECT
        Year,
        Month,
        CrimeCount,
        Rank
    FROM (
        SELECT
            YEAR(DATE(`DATE OCC`)) AS Year,
            MONTH(DATE(`DATE OCC`)) AS Month,
            COUNT(*) AS CrimeCount,
            DENSE_RANK() OVER (PARTITION BY YEAR(DATE(`DATE OCC`)) ORDER BY COUNT(*) DESC) AS Rank
        FROM crime_data
        GROUP BY YEAR(DATE(`DATE OCC`)), MONTH(DATE(`DATE OCC`))
    )
    WHERE Rank <= 3
    ORDER BY Year ASC, CrimeCount DESC;
    """

    logger.info("Execute the query.")
    result = session.sql(query)

    return result


def use_dataframe(df: DataFrame, logger: Logger) -> DataFrame:
    """Executes a DataFrame query to get the top 3 months for each year with the highest number of crimes.

    Args:
        df (DataFrame): The DataFrame containing the crime data.
        logger (Logger): The logger object.

    Returns:
        DataFrame: The results of the DataFrame query.
    """
    logger.info("Add year and month columns.")
    crime_data = df.withColumn("Year", year("DATE OCC")).withColumn(
        "Month", month("DATE OCC")
    )

    logger.info("Get the top 3 months for each year with the highest number of crimes.")
    crime_counts = (
        crime_data.groupBy("Year", "Month")
        .agg(count("*").alias("CrimeCount"))
        .withColumn(
            "Rank",
            dense_rank().over(
                Window.partitionBy("Year").orderBy(col("CrimeCount").desc())
            ),
        )
    )

    logger.info(
        "Filter the top 3 months for each year with the highest number of crimes."
    )
    filtered_data = crime_counts.filter(col("Rank") <= 3)

    logger.info("Get the top 3 months for each year with the highest number of crimes.")
    result = filtered_data.select("Year", "Month", "CrimeCount", "Rank").orderBy(
        "Year", col("CrimeCount").desc()
    )

    return result


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        argparse.Namespace: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file-format",
        choices=["csv", "parquet"],
        default="csv",
        required=False,
        help="Read from CSV file or Parquet.",
    )
    parser.add_argument(
        "--api",
        choices=["sql", "df"],
        default="df",
        required=False,
        help="Use SQL or DataFrame API.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    api, file_format = args.api, args.file_format

    with spark("Query 1") as session:
        logger = get_logger(session)

        if file_format not in ["csv", "parquet"]:
            raise NotImplementedError(f"Invalid file format: {file_format}")

        with timer(f"Reading {file_format.title()} file"):
            df = read_crime_data(session, format=file_format)

        with timer(f"Using the {api.upper()} API"):
            if args.api == "df":
                result = use_dataframe(df, logger)
            elif args.api == "sql":
                result = use_sql(session, df, logger)
            else:
                raise NotImplementedError(f"Invalid API: {args.api}")

            result.show(truncate=False)

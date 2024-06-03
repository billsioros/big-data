from logging import Logger

from helpers import get_logger, read, spark, write
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def combine_crime_data(session: SparkSession, logger: Logger) -> None:
    """Combines the crime data from 2010 to 2019 and 2020 onwards into a single DataFrame.

    Args:
        session (SparkSession): The SparkSession object.
        logger (Logger): The logger object.
    """
    logger.info("Reading crime data from 2010 to 2019.")
    df1 = read(session, "crime_data/crime_data_2010_2019.csv")

    logger.info("Reading crime data from 2020 onwards.")
    df2 = read(session, "crime_data/crime_data_2020_present.csv")

    df = df1.union(df2)

    logger.info("Write the combined DataFrame to a CSV file.")
    write(df, "crime_data.csv")

    logger.info("Write the combined DataFrame to a Parquet file.")
    write(df, "crime_data.parquet")


def combine_la_income_data(session: SparkSession, logger: Logger) -> None:
    """Combines the LA income data from 2015 to 2021 into a single DataFrame.

    Args:
        session (SparkSession): The SparkSession object.
        logger (Logger): The logger object.
    """
    filepaths = [
        ("income/LA_income_2015.csv", 2015),
        ("income/LA_income_2017.csv", 2017),
        ("income/LA_income_2019.csv", 2019),
        ("income/LA_income_2021.csv", 2021),
    ]

    logger.info("Load each CSV file into a separate DataFrame and add the year column.")
    dfs = [
        read(session, filepath).withColumn("Year", lit(year))
        for filepath, year in filepaths
    ]

    logger.info("Combine all DataFrames into a single DataFrame.")
    df = dfs[0]
    for df in dfs[1:]:
        df = df.union(df)

    logger.info("Write the combined DataFrame to a CSV file.")
    write(df, "LA_income.csv")

    logger.info("Write the combined DataFrame to a Parquet file.")
    write(df, "LA_income.parquet")


if __name__ == "__main__":
    with spark("CSV to Parquet") as session:
        logger = get_logger(session)

        combine_crime_data(session, logger)
        combine_la_income_data(session, logger)

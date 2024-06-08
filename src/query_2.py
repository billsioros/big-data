import argparse
from helpers import get_logger, spark, timer
from pyspark.sql import DataFrame
from logging import Logger
from pyspark.sql.functions import col, udf
from helpers import read_crime_data

def TimeToPartOfDay(hour : int) -> str:
    """Converts an hour to a part of the day.

    Args:
        hour (int): The hour to convert.
    
    Returns:
        str: The part of the day corresponding to the given hour.
    """
    if hour >= 5 and hour < 12:
        return 'Morning'
    elif hour >= 12 and hour < 17:
        return 'Afternoon'
    elif hour >= 17 and hour < 21:
        return 'Evening'
    else:
        return 'Night'

def use_dataframe(df: DataFrame, logger: Logger) -> DataFrame:
    """Executes a DataFrame query to get the number of crimes that occurred on the street in each part of the day.

    Args:
        df (DataFrame): The DataFrame containing the crime data.
        logger (Logger): The logger object.
    
    Returns:
        DataFrame: The results of the DataFrame query.
    """

    # Function to convert hour to the part of the day
    time_to_part_of_day = udf(TimeToPartOfDay)

    
    logger.info("Filter the DataFrame to include only crimes that occurred premis STREET.")
    logger.info("Add a new column 'PartOfDay' to the DataFrame based on the 'HOUR OCC' column.")
    logger.info("Group the DataFrame by 'PartOfDay' and count the number of crimes in each group.")

    parts = df.filter(col("Premis Desc") == "STREET") \
        .withColumn("PartOfDay", time_to_part_of_day(col("HOUR OCC"))) \
        .groupBy("PartOfDay") \
        .count() \
        .orderBy("count", ascending=False)
    
    return parts

def use_rdd(df: DataFrame, logger: Logger) -> DataFrame:
    """Executes an RDD query to get the number of crimes that occurred on the street in each part of the day.

    Args:
        df (DataFrame): The DataFrame containing the crime data.
        logger (Logger): The logger object.
    
    Returns:
        DataFrame: The results of the RDD query casted back to a DataFrame.
    
    """

    logger.info("Get RDD")
    logger.info("Filter the RDD to include only crimes that occurred premis STREET.")
    logger.info("Map the RDD to a key-value pair RDD with the key being the part of the day and the value being 1.")
    logger.info("Reduce the RDD by key with sum to get the count of crimes in each part of the day.")

    parts = df.rdd \
            .filter(lambda x: x['Premis Desc'] == "STREET") \
            .map(lambda x: (TimeToPartOfDay(x[3].hour), 1)) \
            .reduceByKey(lambda x,y: x+y) \
            .sortBy(lambda x: x[1], ascending=False)
    
    return parts.toDF(["PartOfDay", "Count"])

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        argparse.Namespace: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api",
        choices=["df", "rdd"],
        default="df",
        required=False,
        help="Use SQL or RDD API.",
    )
    return parser.parse_args()



if __name__ == "__main__":
    args = parse_arguments()
    api = args.api
    format = 'csv'
    
    with spark(f"Query 2 ({api})") as session:
        logger = get_logger(session)

        if api not in ["df", "rdd"]:
            raise NotImplementedError(f"Invalid API: {api}")
        


        with timer(f"Using {api.upper()} API"):
            crime_data = read_crime_data(session, format=format)

            if api == "df":
                result = use_dataframe(crime_data, logger)
            elif api == "rdd":
                result = use_rdd(crime_data, logger)

            result.show()
            
            

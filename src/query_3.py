import argparse
from helpers import get_logger, spark, timer, read, read_crime_data
from logging import Logger

from pyspark.sql.functions import col, udf, split, year, regexp_replace
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DoubleType
from typing import Tuple

def query_3(session: SparkSession, logger: Logger, joinstrategy1: str, joinstrategy2: str) -> Tuple[DataFrame, DataFrame]:
    """Load crime data, filter those in 2015 with a valid victim descent, join with revgecoding data to get ZIP codes,
        get top and bottom 3 zip codes based on income from the income data in 2015 and join with crime data to get the
        crimes in those zip codes, group by Victim Descent and count the number of crimes for each descent.

    Args:
        session (SparkSession): The SparkSession object.
        logger (Logger): The Logger object.
        joinstrategy1 (str): The join strategy for joining crime data with revgecoding data.
        joinstrategy2 (str): The join strategy for joining top/bottom 3 zip codes with crime data.

    Returns:
        Tuple[DataFrame, DataFrame]: A tuple of DataFrames containing the results for the top and bottom 3 zip codes.
    """
    
    logger.info('Reading Crime Data')
    crime_data = read_crime_data(session, format="csv")
    # crime_data.printSchema()

    logger.info('Filtering crime data that happened in 2015 and have a Victim Descent')
    crime_data = crime_data.filter((year(col("DATE OCC")) == 2015) & (col('Vict Descent').isNotNull()))

    # reverse geocoding is read as doubles so we cast these
    logger.info('Casting LAT and LON to double')
    crime_data.withColumn('LAT', col('LAT').cast(DoubleType())) \
        .withColumn('LON', col('LON').cast(DoubleType()))

    
    crime_data = crime_data.select('LAT', 'LON', 'Vict Descent')
    crime_data = crime_data
    
    # Read reverse geocoding data, provides the mapping (LAT, LON) -> (ZIPs)     
    logger.info('Reading revgecoding data')
    revgecoding = read(session, "revgecoding.csv")
    
    # Multiple zips are given in the form ZIP1, ZIP2, or ZIP1-ZIP2 so we keep only the first ZIP by splitting
    # at the first non integer character and keeping the first part
    logger.info('Keeping only the first ZIP code from the ZIP column in revgecoding data')
    split_reg = "[^0-9]"
    revgecoding = revgecoding.withColumn('ZIPcode', split(col('ZIPcode'), split_reg)[0])
    

    # This is an inner join, we do not care about crimes that cannot match to a zip code.
    logger.info('Joining crime data with revgecoding data')
    crime_data_with_zip = crime_data.hint(joinstrategy1).join(revgecoding, ['LAT', 'LON'], "inner")
    crime_data_with_zip.explain(extended=True) # uses broadcast hash join here
    
    

    logger.info('Reading income data for 2015')
    income_2015 = read(session, "income/LA_income_2015.csv")
    
    # Income is given in the form $X,X so we remove the $ and , and cast to double
    logger.info('Cleaning the Estimated Median Income column')
    income_2015 = income_2015.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), "[$,]", "").cast(DoubleType()))
    

    logger.info('Getting the top 3 ZipCodes by income')
    topZIPs = income_2015.orderBy('Estimated Median Income', ascending=False).limit(3)

    logger.info('Getting the bottom 3 ZipCodes by income')
    bottomZIPs = income_2015.orderBy('Estimated Median Income', ascending=True).limit(3)



    # The join is inner we do not care about crime in zip codes that are not in the top/bottom 3
    logger.info('Joining crime data with top 3 ZipCodes')
    crime_data_with_top_zip_n_income = crime_data_with_zip.hint(joinstrategy2).join(topZIPs, crime_data_with_zip['ZIPcode'] == topZIPs['Zip Code'], how="inner")
    crime_data_with_top_zip_n_income.explain(extended=True)  # uses broadcast hash join here by default

    logger.info('Joining crime data with bottom 3 ZipCodes')
    crime_data_with_bottom_zip_n_income = crime_data_with_zip.hint(joinstrategy2).join(bottomZIPs, crime_data_with_zip['ZIPcode'] == bottomZIPs['Zip Code'], how="inner")
    crime_data_with_bottom_zip_n_income.explain(extended=True) # uses broadcast hash join here by default


    logger.info('Grouping by Victim Descent and counting the number of crimes')
    top_results = crime_data_with_top_zip_n_income.groupBy('Vict Descent').count().orderBy('count', ascending=False)  \
        .select('Vict Descent', 'count')

    bottom_results = crime_data_with_bottom_zip_n_income.groupBy('Vict Descent').count().orderBy('count', ascending=False) \
        .select('Vict Descent', 'count')
    

    # Mapping the descent codes to their names 
    descent_to_name_dict = {
        'A' : 'Other Asian', 'B' : 'Black', 'C' : 'Chinese', 'D' : 'Cambodian',
        'F' : 'Filipino', 'G' : 'Guamanian', 'H' : 'Hispanic/Latin/Mexican',
        'I' : 'American Indian/Alaskan Native', 'J' : 'Japanese', 'K' : 'Korean',
        'L' : 'Laotian', 'O' : 'Other', 'P' : 'Pacific Islander', 
        'S' : 'Samoan', 'U' : 'Hawaiian', 'V' : 'Vietnamese', 'W' : 'White',
        'X' : 'Unknown', 'Z' : 'Asian Indian'
    }

    f = udf(lambda x: descent_to_name_dict[x])

    
    logger.info('Mapping Victim Descent codes to their names')
    top_results = top_results.withColumn('Vict Descent', f(col('Vict Descent')))
    bottom_results = bottom_results.withColumn('Vict Descent', f(col('Vict Descent')))

    return top_results, bottom_results

    

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        argparse.Namespace: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--join1",
        choices=["BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"],
        default="",
        required=False,
        help="Join Stategy for LAT, LON with Zip Data",
    )
    parser.add_argument(
        "--join2",
        choices=["BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"],
        default="",
        required=False,
        help="Join Strategy for Top/Bottom 3 Zip Codes with Crime Data",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    joinstrategy1 = args.join1
    joinstrategy2 = args.join2

    
    with spark(f"Query 3 ({joinstrategy1}, {joinstrategy2})") as session:
        logger = get_logger(session)

        if joinstrategy1 not in ["", "BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"]:
            raise NotImplementedError(f"Invalid join: {joinstrategy1}")
        
        if joinstrategy2 not in ["", "BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"]:
            raise NotImplementedError(f"Invalid join: {joinstrategy2}")
        

        with timer(f"Using join strategies: {'default' if joinstrategy1 == '' else joinstrategy1 } and {'default' if joinstrategy2 == '' else joinstrategy2}"):
            top_results, bottom_results = query_3(session, logger, joinstrategy1, joinstrategy2)
            
            top_results.show()
            bottom_results.show()

            
            

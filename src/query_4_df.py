from helpers import get_logger, spark, timer, read, read_crime_data
from logging import Logger

from pyspark.sql.functions import col, udf
from pyspark.sql import SparkSession, DataFrame
from geopy.distance import geodesic

def get_distance(lat1: float, long1: float, lat2: float, long2: float) -> float:
    """Calculate the distance in kilometers between two geographic points.

    Args:
        lat1 (float): Latitude of the first point.
        lon1 (float): Longitude of the first point.
        lat2 (float): Latitude of the second point.
        lon2 (float): Longitude of the second point.
    
    Returns:
        float: The distance in kilometers between the two points.
    """
    return geodesic((lat1 , long1), (lat2 , long2)).km

def query_4_df(session: SparkSession, logger: Logger) -> DataFrame:
    """Load crime data, filter those that are about guns (Weapon Used Cd = 1xx), join with LAPD data to get the
        police station for each crime, calculate the distance between the crime and the police station, group by
        division and calculate the average distance and the total number of incidents.
    
    Args:
        session (SparkSession): The SparkSession object.
        logger (Logger): The Logger object.
    
    Returns:
        DataFrame: The results of the query.
    """

    logger.info('Reading Crime Data')
    crime_data = read_crime_data(session, format="csv")


    # filter those that are about guns (Weapon Used Cd = 1xx)
    logger.info('Filtering crime data that are about guns (Weapon Used Cd = 1xx)')
    crime_data = crime_data.filter((col('Weapon Used Cd').rlike('1..')) & (col('LAT') != 0) & (col('LON') != 0)).select('LAT', 'LON', 'AREA', 'Weapon Used Cd')

    logger.info('Reading LAPD data')
    lapd_data = read(session, "la_police_stations.csv")
    

    logger.info('Joining crime data with LAPD data')
    crime_data_with_lapd = crime_data.join(lapd_data, crime_data['AREA'] == lapd_data['PREC'], how="inner")
    crime_data_with_lapd.explain(extended=True)
    get_distance_udf = udf(get_distance)
    
    logger.info('Calculating the distance between the crime and the police station in a new column')
    crime_data_with_lapd = crime_data_with_lapd.withColumn('Distance', get_distance_udf(col('LAT'), col('LON'), col('Y'), col('X')))

    # average distance is in km
    logger.info('Grouping the data by LAPD division and calculating the average distance and the total number of incidents')
    results = crime_data_with_lapd.groupBy('DIVISION').agg({'Distance': 'avg', 'Weapon Used Cd': 'count'}) \
        .withColumnRenamed('DIVISION', 'division') \
        .withColumnRenamed('avg(Distance)', 'average_distance') \
        .withColumnRenamed('count(Weapon Used Cd)', 'incidents_total') \
        .orderBy('incidents_total', ascending=False)

    return results


if __name__ == "__main__":
    
    with spark(f"Query 4 (DF)") as session:
        logger = get_logger(session)
        

        with timer():
            results = query_4_df(session, logger)
            results.show(results.count())

            
            

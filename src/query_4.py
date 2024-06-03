import argparse
import csv
from io import StringIO
from logging import Logger
from typing import List, Tuple

from geopy.distance import geodesic
from helpers import get_logger, spark, timer
from pyspark.sql import SparkSession


def distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the distance in kilometers between two geographic points.

    Args:
        lat1 (float): Latitude of the first point.
        lon1 (float): Longitude of the first point.
        lat2 (float): Latitude of the second point.
        lon2 (float): Longitude of the second point.

    Returns:
        float: The distance in kilometers between the two points.
    """
    return geodesic((lat1, lon1), (lat2, lon2)).km


def calculate_distance(record: tuple) -> tuple:
    """Calculate the distance in kilometers between a crime location and a police station.

    Args:
        record (tuple): A tuple of (crime, station)
        crime (dict): A dictionary representing a crime
        station (dict): A dictionary representing a police station

    Returns:
        tuple: A tuple of (division, (1, distance))
    """

    crime, station = record[1]
    crime_location = (crime[-2], crime[-1])
    station_location = (station[1], station[0])
    division = station[3]

    return (division, (1, distance(*crime_location, *station_location)))


def parse_csv_line(line: str) -> List[str]:
    """Parse a CSV line string into a list of strings.

    Args:
        line (str): A string representing a CSV line.

    Returns:
        List[str]: A list of strings parsed from the CSV line.

    This function takes a string representing a CSV line and parses it into a list of strings.
    It uses the csv.reader function from the csv module to read the line as a CSV file
    object, and then extracts the first row from the file object using indexing.
    """

    return next(csv.reader(StringIO(line), delimiter=","))


def query(session: SparkSession, logger: Logger) -> List[Tuple[str, Tuple[int, float]]]:
    """Load crime data and police station data, filter relevant data, join the data, compute the average distance,
    sort the results by number of incidents in descending order and return the results.

    Args:
        session (SparkSession): The SparkSession object.
        logger (Logger): The logger object.

    Returns:
        List[Tuple[str, Tuple[int, float]]]: A list of tuples where each tuple contains the division of a police
        station and a tuple containing the number of incidents and the average distance to the station.
    """
    logger.info("Load data.")
    crime_data = session.sparkContext.textFile("./data/crime_data.csv")
    police_stations = session.sparkContext.textFile("./data/la_police_stations.csv")

    logger.info("Filter headers and parse data.")
    header_crime = crime_data.first()
    header_station = police_stations.first()

    crime_data = crime_data.filter(lambda line: line != header_crime).map(
        parse_csv_line
    )
    police_stations = police_stations.filter(lambda line: line != header_station).map(
        parse_csv_line
    )

    logger.info("Filter firearm-related crimes.")
    firearm_crimes = crime_data.filter(
        lambda crime: crime[header_crime.split(",").index("Weapon Used Cd")].startswith(
            "1"
        )
        and (
            float(crime[header_crime.split(",").index("LAT")]) != 0
            and float(crime[header_crime.split(",").index("LON")]) != 0
        )
    )

    logger.info("Convert police stations to a key-value pair RDD for join operation.")
    stations_kv = police_stations.map(
        lambda station: (station[header_station.split(",").index("PREC")], station)
    )

    logger.info("Convert crime data to key-value pair RDD for join operation.")
    crimes_kv = firearm_crimes.map(
        lambda crime: (crime[header_crime.split(",").index("AREA")], crime)
    )

    logger.info("Join crime data with police station data.")
    joined_data = crimes_kv.join(stations_kv)

    logger.info("Map and reduce to get the count and sum of distances.")
    division_stats = joined_data.map(calculate_distance).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    logger.info("Calculate average distance.")
    average_distance = division_stats.mapValues(lambda x: (x[0], x[1] / x[0]))

    logger.info("Sort by number of incidents in descending order.")
    sorted_results = average_distance.sortBy(lambda x: -x[1][0])

    logger.info("Collect results.")
    return sorted_results.collect()


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        argparse.Namespace: The parsed command line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--join",
        choices=["broadcast", "repartition"],
        default="broadcast",
        required=False,
        help="Use broadcast or repartition join.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    join = args.join

    with spark("Query 4 (RDD)") as session:
        logger = get_logger(session)

        if join not in ["broadcast", "repartition"]:
            raise NotImplementedError(f"Invalid join: {join}")

        with timer(f"Using {join} join"):
            print(query(session, logger))

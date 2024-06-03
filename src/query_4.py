import argparse
import csv
from io import StringIO
from logging import Logger
from typing import Any, Callable, Iterator, List, Tuple

from geopy.distance import geodesic
from helpers import get_logger, spark, timer
from pyspark import RDD
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


def broadcast_join(rdd_r: RDD, rdd_l: RDD) -> RDD:
    """Performs a broadcast join between RDDs R and L.

    Args:
        rdd_r (RDD[Tuple[str, Any]]): RDD of records from R.
        rdd_l (RDD[Tuple[str, Any]]): RDD of records from L.

    Returns:
        RDD[Tuple[None, Tuple[Any, Any]]]: RDD containing joined records.
    """
    station_map = rdd_l.collectAsMap()
    broadcast_rdd_l = rdd_r.context.broadcast(station_map)

    joined_data = rdd_r.map(
        lambda crime: (crime[0], (crime, broadcast_rdd_l.value.get(crime[0])))
    ).filter(lambda record: record[1][1] is not None)

    return joined_data


def repartition_join(rdd_r: RDD, rdd_l: RDD) -> RDD:
    """Performs a repartition join between RDDs R and L.

    Args:
        rdd_r (RDD): RDD of records from R.
        rdd_l (RDD): RDD of records from L.

    Returns:
        RDD: RDD containing joined records.
    """
    tagged_rdd_r = rdd_r.map(lambda record: (record[0], ("R", record)))
    tagged_rdd_l = rdd_l.map(lambda record: (record[0], ("L", record)))
    union_rdd = tagged_rdd_r.union(tagged_rdd_l)

    def join_records(
        iterator: Iterator[Tuple[str, Tuple[str, Any]]],
    ) -> Iterator[Tuple[None, Tuple[Any, Any]]]:
        """Join records from R and L.

        Args:
            iterator (Iterator[Tuple[str, Tuple[str, Any]]]): Iterator of records.

        Yields:
            Tuple[None, Tuple[Any, Any]]: Joined records.
        """
        buffer_r = []
        buffer_l = []
        for _, (tag, record) in iterator:
            if tag == "R":
                buffer_r.append(record)
            else:
                buffer_l.append(record)
        yield from ((None, (r, l)) for r in buffer_r for l in buffer_l)

    return union_rdd.groupByKey().flatMap(join_records)


def query(
    session: SparkSession,
    logger: Logger,
    join_method: Callable[[RDD, RDD], RDD],
) -> List[Tuple[str, Tuple[int, float]]]:
    """Performs a query to calculate the average distance between crime locations and police stations.

    Args:
        session (SparkSession): SparkSession object.
        logger (Logger): Logger object.
        join_method (Callable[[RDD, RDD], RDD]): Join method to use (broadcast or repartition).

    Returns:
        List[Tuple[str, Tuple[int, float]]]: List of tuples containing the division name,
        a tuple of the number of incidents and the average distance.
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

    logger.info("Convert crime data to a key-value pair RDD for join operation.")
    crimes_kv = firearm_crimes.map(
        lambda crime: (crime[header_crime.split(",").index("AREA")], crime)
    )

    logger.info("Performing repartition join.")
    joined_data = join_method(crimes_kv, stations_kv)

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

    with spark("Query 4 (RDD)") as session:
        logger = get_logger(session)

        join_method = None
        if args.join == "broadcast":
            join_method = broadcast_join
        elif args.join == "repartition":
            join_method = repartition_join
        else:
            raise NotImplementedError(f"Invalid join: {args.join}")

        with timer(f"Using {args.join} join"):
            print(query(session, logger, join_method))

import argparse
import csv
from collections import defaultdict
from io import StringIO
from logging import Logger
from typing import Callable, Dict, Iterator, List, Tuple, TypeVar

from geopy.distance import geodesic
from helpers import get_logger, spark, timer
from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession

T = TypeVar("T", covariant=True)


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


def calculate_distance(
    record: Tuple[str, Tuple[List[str], List[str]]],
) -> Tuple[str, Tuple[float, int]]:
    """Calculate the distance in kilometers between a crime location and a police station.

    Args:
        record (Tuple[str, Tuple[List[str], List[str]]]): A tuple containing two lists, each
        representing a crime and a police station respectively. The lists contain the fields of
        the respective entities in the same order as they appear in the dataset.

    Returns:
        Tuple[str, Tuple[float, int]]: A tuple containing the division of the police station
        and the distance and a constant value (1) in a tuple.
    """
    station, crime = record[1]
    crime_location = (float(crime[-2]), float(crime[-1]))
    station_location = (float(station[1]), float(station[0]))
    division = station[3]
    distance_km = distance(*crime_location, *station_location)

    return (division, (distance_km, 1))


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


def broadcast_join(
    rdd_l: RDD[List[str]],
    rdd_r: RDD[List[str]],
    join_indices: Tuple[int, int],
) -> RDD[Tuple[str, Tuple[List[str], List[str]]]]:
    """Performs a broadcast join between RDDs R and L.

    Args:
        rdd_l (RDD[List[str]]): RDD of records from R.
        rdd_r (RDD[List[str]]): RDD of records from L.
        join_indices (Tuple[int, int]): Indices of the join key in R and L records.

    Returns:
        RDD[Tuple[str, Tuple[List[str], List[str]]]]: RDD containing joined records.

    Performs a broadcast join between RDDs R and L. It collects R records into a dictionary
    where the key is the join key and the value is a list of records. This dictionary is then
    broadcasted to all worker nodes. For each record in L, it checks if the join key exists in the
    broadcasted dictionary. If it does, it performs the join operation by combining the records
    from R and L.
    """
    l_index, r_index = join_indices

    r_dict: Dict[str, List[str]] = defaultdict(list)
    for record in rdd_r.collect():
        r_key = record[r_index]
        r_dict[r_key].append(record)

    r_broadcast = rdd_r.context.broadcast(r_dict)

    def mapper(
        record: List[str],
    ) -> Iterator[Tuple[str, Tuple[List[str], List[str]]]]:
        """Map function to be applied to each record in RDD L.

        Args:
            record (List[str]): A record from RDD L.

        Returns:
            Iterator[Tuple[str, Tuple[List[str], List[str]]]]: List of joined records.

        This function takes a record from RDD L, retrieves the join key, looks up the
        corresponding records from R in the broadcast dictionary, and generates the
        joined records.
        """
        join_key = record[l_index]
        r_dict = r_broadcast.value

        yield from ((join_key, (match, record)) for match in r_dict[join_key])

    return rdd_l.flatMap(mapper)


def repartition_join(
    rdd_l: RDD[List[str]],
    rdd_r: RDD[List[str]],
    join_indices: Tuple[int, int],
) -> RDD[Tuple[str, Tuple[List[str], List[str]]]]:
    """Performs a repartition join between RDDs R and L.

    Args:
        rdd_l (RDD[List[str]]): RDD of records from R.
        rdd_r (RDD[List[str]]): RDD of records from L.
        join_indices (Tuple[int, int]): Indices of the join key in R and L records.

    Returns:
        RDD[Tuple[str, Tuple[List[str], List[str]]]]: RDD containing joined records.

    This function performs a repartition join between two RDDs by joining the records
    from R and L on the key. The join is performed by tagging the records from R and L
    with a tag indicating from which RDD they came from, and then performing a union of
    the two tagged RDDs. The resulting RDD is then grouped by key and the reduce phase
    function is applied to each group. The reduce phase function joins the records from
    R and L by filling two buffers: one for records from R and one for records from L.
    The joined records are then generated by iterating through the records in the buffer
    for R and L.

    Each record is a tuple of the form (tag, record), where tag is either "R"
    or "L", indicating from which RDD the record came from, and record is a
    tuple of the form (key, value), where key is the join key and value is the
    value associated with the join key.
    """

    tagged_rdd_l = rdd_l.map(lambda record: (record[join_indices[0]], ("L", record)))
    tagged_rdd_r = rdd_r.map(lambda record: (record[join_indices[1]], ("R", record)))

    union_rdd = tagged_rdd_l.union(tagged_rdd_r)

    def mapper(
        join_key: str, records: Iterator[Tuple[str, Tuple[str, List[str]]]]
    ) -> Iterator[Tuple[str, Tuple[List[str], List[str]]]]:
        """Join records from R and L.

        Args:
            join_key (str): The join key.
            records (Iterator[Tuple[str, Tuple[str, List[str]]]]): Iterator of
                records.

        Yields:
            Iterator[Tuple[str, Tuple[List[str], List[str]]]]: Joined records.

        This function joins the records from R and L by filling two buffers: one
        for records from R and one for records from L. It then generates the joined
        records by iterating through the records in the buffer for R and L.

        Each record is a tuple of the form (tag, record), where tag is either "R"
        or "L", indicating from which RDD the record came from, and record is a
        tuple of the form (key, value), where key is the join key and value is the
        value associated with the join key.
        """

        buffer_r: List[List[str]] = []
        buffer_l: List[List[str]] = []

        for tag, record in records:
            if tag == "R":
                buffer_r.append(record)
            else:
                buffer_l.append(record)

        yield from ((join_key, (r, l)) for r in buffer_r for l in buffer_l)

    results = union_rdd.groupByKey().flatMap(lambda x: mapper(x[0], x[1]))

    return results


def query(
    session: SparkSession,
    logger: Logger,
    join_method: Callable[
        [RDD[List[str]], RDD[List[str]], Tuple[int, int]],
        RDD[Tuple[str, Tuple[List[str], List[str]]]],
    ],
) -> DataFrame:
    """Performs a query to calculate the average distance between crime locations and police stations.

    Args:
        session (SparkSession): The SparkSession object used to create the DataFrame.
        logger (Logger): The logger object used for logging.
        join_method (Callable[
            [RDD[List[str]], RDD[List[str]], Tuple[int, int]],
            RDD[Tuple[str, Tuple[List[str], List[str]]]]]): The join method to use (broadcast or repartition).

    Returns:
        DataFrame: A DataFrame containing the division name, the average distance, and the number of incidents.
    """
    logger.info("Load data.")
    ctx = session.sparkContext
    crime_data: RDD[str] = ctx.textFile("./data/crime_data.csv")
    police_stations: RDD[str] = ctx.textFile("./data/la_police_stations.csv")

    logger.info("Filter headers and parse data.")
    crime_data_header = crime_data.take(1)[0]
    police_stations_header = police_stations.take(1)[0]

    crime_cols: Dict[str, int] = {
        column: i for i, column in enumerate(crime_data_header.split(","))
    }

    police_cols: Dict[str, int] = {
        column: i for i, column in enumerate(police_stations_header.split(","))
    }

    crime_data: RDD[List[str]] = crime_data.map(parse_csv_line)
    police_stations: RDD[List[str]] = police_stations.map(parse_csv_line)

    logger.info("Filter firearm-related crimes.")
    crime_data = crime_data.filter(
        lambda crime: crime[crime_cols["Weapon Used Cd"]].startswith("1")
    )

    logger.info("Filter crimes wrongly located in Null Island.")
    crime_data = crime_data.filter(
        lambda crime: (
            float(crime[crime_cols["LAT"]]) != 0
            and float(crime[crime_cols["LON"]]) != 0
        )
    )

    logger.info("Performing join.")
    join_indices = [crime_cols["AREA"], police_cols["PREC"]]
    joined_data = join_method(crime_data, police_stations, join_indices)

    logger.info("Map and reduce to get the count and sum of distances.")
    results = joined_data.map(calculate_distance).reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    logger.info("Calculate average distance.")
    results = results.map(lambda x: (x[0], x[1][0] / x[1][1], x[1][1]))

    logger.info("Sort by number of incidents in descending order.")
    results = results.sortBy(lambda x: -x[2])

    logger.info("Collect results.")
    return results.toDF(["Division", "Average Distance", "Incidents"])


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
            result = query(session, logger, join_method)
            result.show(result.count(), truncate=False)

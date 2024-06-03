import timeit
from contextlib import contextmanager
import logging
from typing import Generator, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, hour, lpad, minute, second, to_date, to_timestamp


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s|%(levelname)s] %(filename)s:%(lineno)d: %(message)s",
)


@contextmanager
def spark(
    app_name: str = "PySpark HDFS Example",
    ip: Optional[str] = None,
    log_level: str = "ERROR",
) -> Generator[SparkSession, None, None]:
    """Context manager for creating and managing a SparkSession.

    Args:
        app_name (str, optional): The name of the Spark application.
            Defaults to "PySpark HDFS Example".
        ip (str, optional): The IP address of the VM. Defaults to None.
        log_level (str, optional): The log level for the Spark application.
            Defaults to "ERROR".

    Yields:
        SparkSession: The Spark session object.

    The function sets up the Spark context with the specified application name,
    master URL, and default file system. It also sets the log level and clears
    the cache. The function ensures that the Spark session is stopped after use.
    """

    if ip is None:
        ip = "master"

    session = (
        SparkSession.builder.appName(app_name)
        .master(f"spark://{ip}:7077")
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{ip}:9000")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel(log_level)
    session.catalog.clearCache()

    try:
        yield session
    finally:
        session.stop()


@contextmanager
def timer(context: Optional[str] = None) -> Generator[None, None, None]:
    """Context manager for timing execution.

    This context manager measures the execution time of the code block within it and
    prints the time taken.

    Args:
        context (Optional[str], optional): The context of the timer, which is included
            in the printed message. Defaults to None.
    """

    if context is None:
        context = "Execution completed"

    start_time: float = timeit.default_timer()

    try:
        yield
    finally:
        end_time: float = timeit.default_timer()
        execution_time: float = end_time - start_time

        print(f"{context} ({execution_time:.6f}s)")


def get_logger(session: SparkSession) -> logging.Logger:
    """Get a logger instance for the given SparkSession.

    Args:
        session (SparkSession): The SparkSession for which to get a logger.

    Returns:
        logging.Logger: A logger instance for the given SparkSession.

    This function generates a logger instance with a name composed of the application name
    and the application ID of the given SparkSession.
    """
    return logging.getLogger(session.sparkContext.appName)


def read(
    session: SparkSession,
    filename: str,
    base_dir: str = "data",
    **options: dict,
) -> DataFrame:
    """Reads a file into a Spark DataFrame.

    Args:
        session (SparkSession): The SparkSession object.
        filename (str): The name of the file to read.
        base_dir (str, optional): The base directory for the file. Defaults to "data".
        **options: Additional options to pass to the file reader.

    Returns:
        DataFrame: The DataFrame containing the file data.

    The function reads a file into a Spark DataFrame. It splits the filename to get the file format,
    constructs the file path by combining the base directory and filename, and sets default options.
    It then uses the file format and options to read the file and load it into a DataFrame.
    """
    fmt = filename.split(".")[-1]
    path = f"{base_dir}/{filename}"

    options = {
        "header": "true",
        "escape": '"',
        "delimiter": ",",
        "inferSchema": "true",
        **options,
    }

    return session.read.format(fmt).options(**options).load(path)


def read_crime_data(
    session: SparkSession,
    basename: str = "crime_data",
    format: str = "csv",
) -> DataFrame:
    """Reads crime data from a CSV file and performs some data transformations.

    Args:
        session (SparkSession): The SparkSession object.
        basename (str): The base name of the CSV file to read. Defaults to "crime_data".
        format (str): The file format. Defaults to "csv".

    Returns:
        DataFrame: The DataFrame containing the crime data with added date and time columns.
    """

    return (
        read(session, f"{basename}.{format}")
        .withColumn("Date Rptd", to_date(col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a"))
        .withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
        .withColumn("TIME OCC", to_timestamp(lpad(col("TIME OCC"), 4, "0"), "HHmm"))
        .withColumn("HOUR OCC", hour(col("TIME OCC")))
        .withColumn("MINUTE OCC", minute(col("TIME OCC")))
        .withColumn("SECOND OCC", second(col("TIME OCC")))
    )


def write(
    df: DataFrame,
    filename: str,
    base_dir: str = "data",
    mode: str = "overwrite",
    **options: dict,
) -> None:
    """Writes a Spark DataFrame to a file.

    Args:
        df (DataFrame): The DataFrame to write.
        filename (str): The name of the file to write.
        base_dir (str, optional): The base directory for the file. Defaults to "data".
        mode (str, optional): The write mode. Defaults to "overwrite".
        **options: Additional options to pass to the file writer.

    Returns:
        None

    This function splits the filename to get the file format,
    constructs the file path by combining the base directory and filename, and sets default options.
    It then uses the file format and options to write the DataFrame to the file.
    """

    fmt = filename.split(".")[-1]

    path = f"{base_dir}/{filename}"

    options = {
        "header": "true",
        "escape": '"',
        "delimiter": ",",
        "inferSchema": "true",
        **options,
    }

    df.write.format(fmt).options(**options).save(path, mode=mode)

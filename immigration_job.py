import os
from datetime import datetime, timedelta

import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id

ALLOW_READ: bool = True  #: read from local or S3
ALLOW_WRITE: bool = True  #: write to S3
S3_BUCKET: str = 's3://datalakebucketjsb/udacity/capstone'

IMMIGRATION_QUERY: str = """
SELECT 
    INT(immigration.i94yr) AS year,
    INT(immigration.i94mon) AS month,
    INT(immigration.i94cit) AS birth_country,
    INT(immigration.i94res) AS residence_country,
    immigration.i94port AS port,
    immigration.arrdate AS arrival_date,
    COALESCE(immigration.i94mode, 'Not reported') AS arrival_mode,
    COALESCE(immigration.i94addr, '99') AS state,
    immigration.depdate AS departure_date,
    FLOAT(immigration.i94bir) AS age,
    COALESCE(immigration.i94visa, 'Other') AS visa_category,
    immigration.dtadfile AS date_added,
    immigration.visapost AS visa_department,
    immigration.occup AS occupation,
    immigration.entdepa AS arrival_flag,
    immigration.entdepd AS departure_flag,
    immigration.entdepu AS update_flag,
    immigration.matflag AS match_flag,
    INT(immigration.biryear) AS birth_year,
    immigration.gender AS gender,
    immigration.insnum AS ins_number,
    immigration.airline AS airline,
    immigration.admnum AS admission_number,
    immigration.fltno AS flight_number,
    immigration.visatype AS visa_type
FROM
    immigration
"""

config = configparser.ConfigParser()

config.read(
    os.path.abspath(
        os.path.join(
            os.path.dirname(
                __file__
            ),
            'dl.cfg'
        ),
    )
)

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def read_addresses(spark: SparkSession, input_data: str):
    """
    Function that reads the addresses data from the S3 bucket
    into the Spark Session

    Parameters
    ----------
    spark
        spark session
    input_data
        path to S3 data
    """

    if ALLOW_READ:
        song_data = f"{input_data}/addresses/addresses.parquet"
    else:
        song_data = "output/addresses.parquet"

    return spark.read.parquet(
        song_data
    )


def read_countries(spark: SparkSession, input_data: str):
    """
    Function that reads the countries data from the S3 bucket
    into the Spark Session

    Parameters
    ----------
    spark
        spark session
    input_data
        path to S3 data
    """

    if ALLOW_READ:
        song_data = f"{input_data}/countries/countries.parquet"
    else:
        song_data = "output/countries.parquet"

    return spark.read.parquet(
        song_data
    )


def read_demographics(spark: SparkSession, input_data: str):
    """
    Function that reads the demographics data from the S3 bucket
    into the Spark Session

    Parameters
    ----------
    spark
        spark session
    input_data
        path to S3 data
    """

    if ALLOW_READ:
        song_data = f"{input_data}/demographics/demographics.parquet"
    else:
        song_data = "output/demographics.parquet"

    return spark.read.parquet(
        song_data
    )


def read_modes(spark: SparkSession, input_data: str):
    """
    Function that reads the modes data from the S3 bucket
    into the Spark Session

    Parameters
    ----------
    spark
        spark session
    input_data
        path to S3 data
    """

    if ALLOW_READ:
        song_data = f"{input_data}/modes/modes.parquet"
    else:
        song_data = "output/modes.parquet"

    return spark.read.parquet(
        song_data
    )


def read_ports(spark: SparkSession, input_data: str):
    """
    Function that reads the ports data from the S3 bucket
    into the Spark Session

    Parameters
    ----------
    spark
        spark session
    input_data
        path to S3 data
    """

    if ALLOW_READ:
        song_data = f"{input_data}/ports/ports.parquet"
    else:
        song_data = "output/ports.parquet"

    return spark.read.parquet(
        song_data
    )


def read_visas(spark: SparkSession, input_data: str):
    """
    Function that reads the visas data from the S3 bucket
    into the Spark Session

    Parameters
    ----------
    spark
        spark session
    input_data
        path to S3 data
    """

    if ALLOW_READ:
        song_data = f"{input_data}/visas/visas.parquet"
    else:
        song_data = "output/visas.parquet"

    return spark.read.parquet(
        song_data
    )


def create_spark_session():
    """
    Function that builds the Spark session with the following configuration:
        - spark.jars.packages
        - org.apache.hadoop:hadoop-aws:2.7.0
    """

    return SparkSession.builder.config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()


def process_immigration_data(
        spark: SparkSession,
        input_data: str,
        output_data: str,
        query: str
):
    """
    Function that process the immigration data and builds the parquet table:
        - immigration/immigration

    This table is uploaded to S3

    Parameters
    ----------
    spark
        Spark Session
    input_data
        Path to S3 input data
    output_data
        Path to S3 Parquet output
    """

    immigration = spark.read.option(
        "header",
        True
    ).csv(
        os.path.abspath(
            os.path.join(
                os.path.dirname(
                    __file__
                ),
                'data',
                'immigration_data_sample.csv'
            ),
        )
    )

    get_date = udf(
        lambda x: (
                datetime(1960, 1, 1).date() + timedelta(int(x))
        ).isoformat() if x else None
    )

    immigration = immigration.withColumn(
        "arrdate", get_date(immigration.arrdate)
    )

    immigration.createOrReplaceTempView(
        "immigration"
    )

    cleaned_df = spark.sql(
        query
    ).dropDuplicates(
        [
            'admission_number'
        ]
    ).withColumn(
        "id",
        monotonically_increasing_id()
    )

    # write songplays table to parquet files partitioned by year and month
    if ALLOW_WRITE:
        cleaned_df.write.partitionBy(
            "year",
            "month",
            "state"
        ).parquet(
            f"{output_data}/immigration/immigration.parquet"
        )
    else:
        cleaned_df.write.partitionBy(
            "year",
            "month",
            "state"
        ).parquet(
            os.path.abspath(
                os.path.join(
                    os.path.dirname(
                        __file__
                    ),
                    'output',
                    'immigration.parquet'
                ),
            )
        )


def main():
    """
    Function that runs the complete Spark job
    """

    spark = create_spark_session()

    process_immigration_data(
        spark,
        S3_BUCKET,
        S3_BUCKET,
        IMMIGRATION_QUERY
    )


if __name__ == "__main__":
    main()

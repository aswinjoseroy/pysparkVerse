"""
task1.py
~~~~~~~~

This module has some logic to process the recipe data. Replace it with your logic and methods

"""
from jobs.helpers.spark_df_transformations import convert_empty_strings_to_null, \
    convert_iso_8601_duration_to_minutes, convert_strings_to_lowercase


def read_data_from_local(spark):
    """
    This method reads data from local endpoint

    :param spark: A reference to the Spark Session
    :return: A Spark DataFrame
    """
    return spark.read.json('../tests/test_data/input.json')


def read_data_from_url(spark, input_data_url):
    """
    This method reads data from a remote endpoint available across all the executors,
    e.g a hdfs file url

    :param spark: A reference to the Spark Session
    :return: A Spark DataFrame
    """
    return spark.read.json(input_data_url)


def read_data_from_s3(spark, config_dict):
    """
    This method reads data from s3 endpoint using configs

    Intentionally left vacant as I do not have an AWS account to setup and test

    :param spark: A reference to the Spark Session
    :return: A Spark DataFrame
    """
    return None


def preprocess_data(data):
    """

    The method that preprocesses the input data

    :param data: input spark dataframe
    :return: processed spark dataframe
    """
    # TODO Business logic is now split between the preprocessing and the transformation steps.
    # TODO It should ideally be concentrated in the transformation step. The preprocessing step should only ingest
    # TODO the data and persist it in a more performant format, like Parquet.
    data = convert_empty_strings_to_null(data)
    data = convert_iso_8601_duration_to_minutes(data)
    data = convert_strings_to_lowercase(data)
    return data


def run_task1(spark=None, logger=None, config_dict=None):
    """

    task 1 reads data from source and pre-processes the data for further optimised performance

    :param spark: A reference to the Spark Session
    :param logger: A reference to the logging object
    :param config_dict: A reference to the configuration loaded from config json
    :return: A Spark DataFrame
    """

    # do some kind of transformation
    logger.warn("Input data read from source. Starting pre-processing..")
    return preprocess_data(input_df)

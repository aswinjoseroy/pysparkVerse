"""
spark.py
~~~~~~~~

This module has helper functions for use with Apache Spark
"""

import json
from os import listdir, path

from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging


def get_dependencies_tuple(spark_app_name='recipe_analysis_spark_app', spark_jar_packages=[],
                           spark_files=[], spark_config={}):
    """This method starts the Spark session, gets Spark logger and loads config spark_files (if any).

    Start a Spark session and register the Spark application with the cluster.
    Only the spark_app_name argument will apply when this is called from a script sent to spark-submit.
    All other arguments are to be passed for testing the script from within a Python console or IDE.

    If a file ending in 'config.json' is found uploaded using --spark_files,
    the contents are parsed (assuming it contains valid JSON ) into a dict of configuration
    parameters, which are returned as the last element in the tuple returned by this function.
    If no file is found, then the config dict is hardcoded to the content for the current assignment's tasks.

    Make sure the caller of this method stops the spark session reference after use.

    :param spark_app_name: The name of Spark app.
    :param spark_master: Cluster master's connection url (defaults to local[*]).
    :param spark_jar_packages: List of Spark JAR package names.
    :param spark_files: List of spark_files to send to Spark cluster's master and workers.
    :param spark_config: Dictionary of config key-value pairs for Spark.
    :return: A tuple of references to the Spark session, logger and config dict.
    """

    # build Spark Session
    # master url not specified as it is to be received via spark-submit command
    spark_builder = (SparkSession
                     .builder
                     .appName(spark_app_name))

    # we are enabling kyro serialization for better optimised serialization
    spark_builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrationRequired", "false")
    # compress serialized RDD partitions
    spark_builder.config("spark.rdd.compress", "true")

    # create Spark JAR packages string, if present
    if spark_jar_packages:
        spark_jars_packages = ','.join(list(spark_jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)
    # create Spark spark_files string, if present
    if spark_files:
        spark_files = ','.join(list(spark_files))
        spark_builder.config('spark.spark_files', spark_files)

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # create session and get Spark logger object
    spark_session = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_session)

    # get config file if sent to cluster with --spark_files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        path_to_config_file = path.join(spark_files_dir, config_files[0])
        with open(path_to_config_file, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.warn('loaded config from ' + config_files[0])
    else:
        # hard coding configs as the application is executed from a python console or IDE
        config_dict = json.loads("""{"ingredient": "beef","input_data_location": "remote_url",
        "input_file_url": "file:///home/aswin/PycharmProjects/hello_fresh_asst/tests/test_data/input.json",
        "task2_output_file_url": "file:///home/aswin/PycharmProjects/hello_fresh_asst/output/result.csv"}""")

    return spark_session, spark_logger, config_dict

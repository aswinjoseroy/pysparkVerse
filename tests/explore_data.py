"""
explore_data.py
~~~~~~~~

This module has helpers to do exploratory analysis on the columns of the input data
"""


def analyse_input_file(spark, logger):
    csv_df = spark.read.json("tests/test_data/input.json")
    logger.warn(csv_df.describe().show())


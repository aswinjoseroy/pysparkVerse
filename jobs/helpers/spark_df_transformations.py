"""
transformations.py
~~~~~~~~

This module has all the spark transformations to carry out the logic for the tasks of the assignment

"""
# TODO write base classes (OR use OOPS) to read and process data

import pyspark.sql.functions as spark_functions
from isodate import parse_duration, duration_isoformat
from datetime import timedelta
from pyspark.sql.functions import when
from pyspark.sql.types import FloatType, StructType, StructField, StringType

from dependencies.constants import ERROR_MESSAGE_VALUE_MISSING


def convert_empty_strings_to_null(input_df):
    """

    Assigns all empty strings in all columns to None

    :param input_df: The input Spark DataFame
    :return:The output Spark DataFame
    """
    expression_blank_strings = [
        blank_as_null_expression(x).alias(x) for x in input_df.columns]

    return input_df.select(*expression_blank_strings)


def blank_as_null_expression(x):
    """
    SQL query building helper for convert_empty_strings_to_null

    :param x: a column name
    :return: sql expression for the column
    """
    return when(spark_functions.col(x) == "", None).otherwise(spark_functions.col(x))


def convert_strings_to_lowercase(input_df, list_of_columns=None):
    """

    Convert selected text fields to lowercase

    :param input_df: The input Spark DataFrame
    :param list_of_columns: Overwrite the list of columns to transform to lowercase
    :return: The output Spark DataFame
    """
    if list_of_columns is None:
        list_of_columns = ['ingredients', 'description', 'recipeYield', ]

    return input_df.select(
        *[spark_functions.lower(spark_functions.col(col_name)).name(col_name)
          if col_name in list_of_columns else spark_functions.col(col_name) for col_name in input_df.columns]
    )


def convert_iso_8601_duration_to_minutes(input_df):
    """

    Convert the values of columns with time durations in ISO 8601 format to minutes in float

    The approach in this method can be used across all transformations with a custom logic.
    A new field in the shape of error_field_name is added to the DF and can later be used for
    analysis of the data quality. The try/catch in the custom udf is what enables this to happen.

    As nested columns in Spark DataFrames cannot be renamed out-of-the-box, we take an approach of
    creating temporary fields (of StructType) which are later converted to appropriate columns.

    :param input_df: The input Spark DataFrame
    :return: The output Spark DataFame
    """
    date_conversion_schema = StructType([
        StructField("minutes", FloatType(), True),
        StructField("error", StringType(), True)
    ])
    time_duration_udf = spark_functions.udf(date_conversion_udf, date_conversion_schema)
    return input_df \
        .withColumn("cook_time", time_duration_udf(input_df.cookTime)) \
        .withColumn("error_cook_time", spark_functions.col("cook_time.error")) \
        .withColumn("cookTime", spark_functions.col("cook_time.minutes")) \
        .withColumn("prep_time", time_duration_udf(input_df.prepTime)) \
        .withColumn("error_prep_time", spark_functions.col("prep_time.error")) \
        .withColumn("prepTime", spark_functions.col("prep_time.minutes")) \
        .drop("cook_time") \
        .drop("prep_time")


def date_conversion_udf(x):
    """

    The udf returns a Tuple with the actual number of minutes in the ISO representation of duration
    and an error message if the conversion failed (which assigns None to the corresponding value).

    :param x: each ISO duration time to be converted to seconds
    :return: the number of corresponding minutes
    """
    error_ = None
    minutes_ = None
    if x is not '' or None:
        try:
            minutes_ = parse_duration(x).total_seconds() / 60
        except Exception as e:
            error_ = str(e)
    else:
        error_ = ERROR_MESSAGE_VALUE_MISSING
    return minutes_, error_

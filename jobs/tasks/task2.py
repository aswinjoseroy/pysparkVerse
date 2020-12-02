"""
task2.py
~~~~~~~~

This module has some logic to process the recipe data. Replace it with your logic and methods

"""
from jobs.helpers import spark_df_transformations


def load_data(df, config_dict):
    """Collect data locally and write to CSV.

    :param df: Spark DataFrame to save.
    :return: None
    """
    if df is None:
        raise Exception("result df is None")
    (df
     .coalesce(1)
     .write
     .csv(config_dict['task2_output_file_url'], mode='overwrite', header=True))
    return None


def run_task_2(input_recipes, spark, logger, config_dict):

    """
    This method runs the task 2 on the input Spark DataFrame

    :param input_recipes: A Spark DataFrame
    :param spark: A reference to the Spark Session
    :param logger: A reference to the logging object
    :param config_dict: A reference to the configuration loaded from config json file
    """
    # caching the total recipes df to MEM_ONLY serialization as it is used multiple times
    input_recipes.cache()

    # task 2
    # do something

    final_result_df = None
    # write result to disk
    load_data(final_result_df, config_dict)
    logger.warn("result data written successfully")

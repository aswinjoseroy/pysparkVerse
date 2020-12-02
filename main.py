from dependencies.spark import get_dependencies_tuple
from jobs.tasks import task1, task2


def main():
    """Main script definition.

    :return: None
    """

    # setup dependencies
    spark, logger, config_dict = get_dependencies_tuple(spark_app_name='recipe_analysis_spark_app')

    # log that main ETL job is starting
    logger.warn('spark is up-and-running, tasks are about to run..')

    # replace read_data_from_source argument's value from `local` to `s3` to read a file from s3
    task1_result = task1.run_task1(spark=spark, logger=logger, config_dict=config_dict)

    # task2 does not return anything
    task2.run_task_2(input_recipes=task1_result, spark=spark, logger=logger, config_dict=config_dict)

    # cleanup
    logger.warn('tasks are finished, shutting down spark session..')
    spark.stop()
    return None


if __name__ == '__main__':
    main()

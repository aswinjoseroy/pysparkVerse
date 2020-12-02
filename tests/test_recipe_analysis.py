"""
test_recipe_analysis.py
~~~~~~~~

This module has unit tests for all the individual logic components of the assignment
"""
import json
import unittest

from dependencies.constants import ERROR_MESSAGE_VALUE_MISSING, ERROR_MESSAGE_ISO_FORMAT_UNRECOGNISED
from dependencies.spark import get_dependencies_tuple
from jobs.helpers import spark_df_transformations


class SparkRecipeAnalysisTestCases(unittest.TestCase):
    """Test suite for transformations in recipe analysis
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @classmethod
    def setUpClass(cls):
        """Start Spark, define config and path to test data
        """
        cls.config = json.loads("""{"ingredient": "beef"}""")
        cls.spark, *_ = get_dependencies_tuple(spark_app_name='test_recipe_analysis_spark_app')

    @classmethod
    def tearDownClass(cls):
        """Stop Spark
        """
        cls.spark.stop()

    def test_convert_strings_to_lowercase(self):
        """
        tests the method to convert strings to all lowercase

        """
        df = self.spark.createDataFrame(data=[[1, 'Milk salt sugar'], [2, 'Beef, Oil, Rice'],
                                              [3, None]],
                                        schema=['c1', 'c2'])
        expected_result = [(1, 'milk salt sugar'), (2, 'beef, oil, rice'), (3, None)]

        self.assertEqual(set(spark_df_transformations.convert_strings_to_lowercase(df, ['c2', ]).collect()),
                         set(expected_result))

    def test_empty_string_cleaning_method(self):
        """
        tests the method to convert all empty strings to None
        """
        df = self.spark.createDataFrame(data=[[1, 'PT10M', 'PT5M'], [2, 'PT1H', 'PT15M'], [3, '', 'PT5M']],
                                        schema=['c1', 'cookTime', 'prepTime'])
        expected_result = [(1, 'PT10M', 'PT5M'), (2, 'PT1H', 'PT15M'), (3, None, 'PT5M')]

        self.assertEqual(set(spark_df_transformations.convert_empty_strings_to_null(df).collect()),
                         set(expected_result))

    def test_time_duration_conversion(self):
        """
        tests the method to convert all ISO 8601 format time durations to minutes

        """
        df = self.spark.createDataFrame(data=[[1, 'PT10M', 'PT5M'], [2, 'PT1H', 'PT15M'], [3, '', 'PT5Miuuyg']],
                                        schema=['c1', 'cookTime', 'prepTime'])
        expected_result = [(1, 10.0, 5.0, None, None), (2, 60.0, 15.0, None, None),
                           (3, None, None, ERROR_MESSAGE_VALUE_MISSING, ERROR_MESSAGE_ISO_FORMAT_UNRECOGNISED)]

        self.assertEqual(set(spark_df_transformations.convert_iso_8601_duration_to_minutes(df).collect()),
                         set(expected_result))

    def test_add_total_cooking_time(self):
        """
        test the method to derive total cooking time
        """
        df = self.spark.createDataFrame(data=[[20, 20], [45, 10], [16, 3], [25, 45], [25, None], ],
                                        schema=['cookTime', 'prepTime'])
        expected_result = [(20, 20, 40), (45, 10, 55), (16, 3, 19), (25, 45, 70), (25, None, None)]

        self.assertEqual(set(spark_df_transformations.add_total_cooking_time(df).collect()),
                         set(expected_result))

    def test_add_difficulty(self):
        """
        test the method to derive difficulty level based on total cooking time
        """
        df = self.spark.createDataFrame(data=[[30, 20], [45, 10], [16, 35], [75, 45], [None, 10]],
                                        schema=['total_cook_time', 'id'])
        expected_result = [(30, 20, 'medium'), (45, 10, 'medium'), (16, 35, 'easy'), (75, 45, 'hard'),
                           (None, 10, None)]

        self.assertEqual(set(spark_df_transformations.add_difficulty(df).collect()),
                         set(expected_result))

    def test_avg_cooking_time_per_level(self):
        """
        test the method to derive avg cooking time per difficulty level
        """
        df = self.spark.createDataFrame(data=[["easy", 20], ["easy", 10], ["medium", 35], ["hard", 45],
                                              [None, None]],
                                        schema=['difficulty', 'cookTime'])
        expected_result = [('medium', 'PT35M'), ('hard', 'PT45M'), ('easy', 'PT15M')]

        self.assertEqual(set(spark_df_transformations.get_avg_cooking_time_per_level(df).collect()),
                         set(expected_result))

    def test_recipe_filter_on_ingredient_method(self):
        """
        test the method to filter recipes based on an included ingredient
        """
        df = self.spark.createDataFrame(data=[[1, 'milk salt sugar'], [2, 'beef, oil, rice'],
                                              [2, None]],
                                        schema=['c1', 'c2'])

        self.assertEqual(spark_df_transformations.get_recipes_with_a_particular_ingredient(
            df, 'c2', self.config['ingredient']).count(), 1)


if __name__ == '__main__':
    unittest.main()

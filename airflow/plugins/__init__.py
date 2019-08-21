from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import airflow.plugins.operators as operators
import airflow.plugins.helpers as helpers

# Defining the plugin class
class SparkifyPlugin(AirflowPlugin):
    name = "sparkify_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]

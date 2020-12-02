This can be considered a standard framework to write spark ETL jobs in Python. The code uses an example code to analyse cooking recipes data.


### Config handling

Any external configs (eg. the ingredient to filter recipes based on, spark cluster configs) can be sent to Spark
 via a separate file - e.g. using the `--files configs/recipe_analysis_config.json` flag with `spark-submit` - 
 containing the configuration in JSON format, which can be parsed into a Python dictionary in one line of code 
 with `json.loads(config_file_contents)`. 
 Testing the code from within a Python interactive console session is also greatly simplified, as all one has to 
 do to access configuration parameters for testing, is to copy and paste the contents of the file - e.g.,

```python
import json

config = json.loads("""{"config_param": "config_value"}""")
```

Please see the `get_dependencies_tuple()` function in `dependencies/spark.py` which in addition to parsing the 
configuration file sent to Spark (and returning it as a Python dictionary), also launches the Spark driver 
program (the application) on the cluster and retrieves the Spark logger at the same time. 

This same `spark.py` file is used by the unit-testing module, `test_recipe_analysis.py`, and `main.py` as well to 
initiate Spark Sessions and run the tests and assignment tasks respectively.  


### Deployment

Run the shell script `build_dependencies.sh` to create a standalone dependency package named _libs_. 
This folder is zipped along with other dependencies (two separate zip files to maintain import statement 
consistencies) by the same schell script. These zip files should be passed on to the spark-submit 
command while running the job on any spark cluster.
 
An example would be as follows.

Assuming that the `$SPARK_HOME` environment variable points to your local Spark installation folder:

```bash
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--packages 'com.somesparkjar.dependency:1.0.0' \
--py-files /path/to/libs.zip,/path/to/jobs.zip \
--files /path/to/configs/recipe_analysis_config.json \
/path/to/main.py
```

The options supplied serve the following purposes:

- `--master local[*]` - the address of the Spark cluster to start the job on. If you have a Spark cluster in operation (either in single-executor mode locally, or something larger in the cloud) and want to send the job there, then modify this with the appropriate Spark IP - e.g. `spark://the-clusters-ip-address:7077`;
- `--packages 'com.somesparkjar.dependency:1.0.0,...'` - Maven coordinates for any JAR dependencies required by the job (e.g. JDBC driver for connecting to a relational database);
- `--files configs/recipe_analysis_config.json` - the (optional) path to any config file that may be required by the job;
- `--py-files *.zip` - archive containing Python dependencies (modules) referenced by the job; and,
- `main.py` - the Python module file containing the main code to run the assignment's tasks.

Edit the config file `recipe_analysis_config.json` appropriately with the correct path to files that are to be uploaded 
on from a remote path acessible to all the executors, like _file:///.._ or _hdfs_. 

note: `spark.cores.max` and `spark.executor.memory` etc can be passed via the config files based on your cluster's resources available.  
 
 ### Tests
 
 Running the python file _test_recipe_analysis.py_ under the folder _tests_ runs the individual tests to 
 test the individual components of the tasks. An IDE like PyCharm can easily run the tests. 
 
 To test with Spark, we use the pyspark Python package, which is bundled with the Spark JARs required to 
 start-up and tear-down a local Spark instance, on a per-test-suite basis. 
 Using pyspark to run Spark is an alternative way of developing with Spark as opposed to using the 
 PySpark shell or spark-submit. 
 
 ### Error handling
 
 Executor level code's errors are handled (to avoid re-computation amidst a failed large job) and saved as 
 a separate column for errors per column. This pattern can be useful for further analysis of data quality. 
 
 ### Performance Tuning
 
 Configs are added to use Kyro serializer for data serialization. DataFrames used multiple times are cached 
 (MEM_ONLY) and compression of Spark RDDs enabled. 
 
 
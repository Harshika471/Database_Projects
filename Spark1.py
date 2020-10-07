# Databricks notebook source
# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import pyspark
import pyspark.sql.functions

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("project", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("page", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("numViews", pyspark.sql.types.IntegerType(), True)
        ])

    # 2. Operation C2: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    # START COMPLETING YOUR CODE FROM HERE, RESPECTING THE PREVIUOS LINES OF CODE

    # 3. Create a new window to order the project page_names by their 'numViews' in decreasing order.
    my_window = pyspark.sql.Window.partitionBy(inputDF["project"]).orderBy(inputDF["numViews"].desc())
   
    # 4. Operation T1 --> Create a new DataFrame with a column 'rank'
    #                     resulting from ranking the rows of the previous DataFrame over the defined window.
    newdf = inputDF.withColumn("rank", pyspark.sql.functions.row_number().over(my_window))
    # 5. Operation T2 --> Create a new DataFrame 'resultDF'  by filtering the previous DataFrame with just the rows having rank 1.
    resultDF = newdf.filter('rank==1').select('project','page','numViews').orderBy('project')
    
    # STOP WRITING CODE HERE, RESPECTING THE NEXT LINES OF CODE

    # 6. Operation A1: We collect the results
    resVAL = resultDF.collect()

    # 7. We print resVAL
    for item in resVAL:
        print(item[0],(item[1],item[2]))

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    pass

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/my_dataset/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir)

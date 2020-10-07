# Databricks notebook source
import pyspark
import pyspark.sql.functions as F

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

    # 3. Operation T1 --> Create a new DataFrame with an column 'country' of each 'project'
    #inputDF.show()
    newDf = inputDF.withColumn('country', F.split(F.col('project'), r'\.').getItem(0))
    # Unfortunately 'project' might contain one or more dots.
    # For those projects containing dots, the country is the substring before the first dot appearance.
    
    # (e.g., column project 'en' --> new column country 'en',
    #        column project 'en.b' --> new column country 'en',
    #        column project 'en.m.voy' --> new column country 'en'
    #  ).
    # 4. Operation T2 --> Create a new DataFrame with a column 'totalViews'
    #                     resulting from grouping the previous DataFrame by country and sum the values of the column numViews.
    
    mydf = newDf.select('page','numViews','country')
    totalViews = mydf.groupBy('country').agg(F.sum('numViews').alias('totalViews'))
    #toatlViews = sum(totalViews)
    #totalViews.show()
    # 5. Operation T3 --> Create a new DataFrame 'resultDF' by ordering the rows of the previuos DataFrame
    #                     in decreasing order of 'totalViews'
    resultDF = totalViews.orderBy('totalViews',ascending=False)
    resultDF.show()
    # STOP WRITING CODE HERE, RESPECTING THE NEXT LINES OF CODE

    # 6. Operation A1: We collect the results
    resVAL = resultDF.collect()

    # 7. We print resVAL
    for item in resVAL:
        print(item)
  
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

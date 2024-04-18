# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook to access External locations & Common Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating widget to get the environment details

# COMMAND ----------

dbutils.widgets.text("Environment","","Enter the environment name : ")
environment = dbutils.widgets.get("Environment")
print(environment)

# COMMAND ----------

# MAGIC %md
# MAGIC ###paths to external locations

# COMMAND ----------

bronze_external_location = spark.sql(f"DESCRIBE EXTERNAL LOCATION `bronze-{environment}-external-location`").select("url").collect()[0][0]
silver_external_location = spark.sql(f"DESCRIBE EXTERNAL LOCATION `silver-{environment}-external-location`").select("url").collect()[0][0]
gold_external_location = spark.sql(f"DESCRIBE EXTERNAL LOCATION `gold-{environment}-external-location`").select("url").collect()[0][0]
checkpoint_external_location = spark.sql(f"DESCRIBE EXTERNAL LOCATION `checkpoint-{environment}-external-location`").select("url").collect()[0][0]
landing_external_location = spark.sql(f"DESCRIBE EXTERNAL LOCATION `landing-{environment}-external-location`").select("url").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ###Common Function

# COMMAND ----------

# MAGIC %md
# MAGIC #####Remove Duplicate Records

# COMMAND ----------

def dropDuplicateRecords(df):

    print("Removing duplicate records...")
    print("\n\n")

    df_deduped = df.dropDuplicates()

    print("*********************************************Removed Duplicate Records!!************************************************")

    return df_deduped

# COMMAND ----------

# MAGIC %md
# MAGIC #####Fill Empty columns with tangible values
# MAGIC

# COMMAND ----------

def fillEmptyColumns(df,columns):

    print("Filling Empty Columns...")
    print("\n\n")

    df_fillEmptyStrings = df.fillna("Unknown",columns)
    df_fillEmptyNumbers = df_fillEmptyStrings.fillna(0,columns)

    print("**************************************************Filled Empty columns!!**************************************************")

    return df_fillEmptyNumbers

# COMMAND ----------

# MAGIC %md
# MAGIC ###Test Functions
# MAGIC

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC #####Testing the remove duplicate function

# COMMAND ----------

def test_dropDuplicateRecords():

    #Arrange
    input_data = [
        Row("This is a demo first line."),
        Row("This is a demo second line."),
        Row("This is a demo first line."),
        Row("this is a demo third lune.")
    ]

    output_data = [
        Row("This is a demo first line."),
        Row("This is a demo second line."),
        Row("this is a demo third lune.")
    ]

    schema = StructType([
        StructField("lines",StringType(), False)
    ])

    input_df = spark.createDataFrame(input_data,schema)

    probable_output_df = spark.createDataFrame(output_data,schema)

    #Act
    actual_output_df = dropDuplicateRecords(input_df)

    #Assert
    try:
        assert actual_output_df.collect() == probable_output_df.collect()
        assert actual_output_df.schema == probable_output_df.schema
        print("Test cases passed for dropDuplicateRecord method!!")

    except:
        print("Some error occurred!!")

test_dropDuplicateRecords()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Testing fillEmptycolumn method

# COMMAND ----------

def test_fillEmptyColumn():

    #Arrange
    input_data = [
        Row("This is a demo 1st Row",24,"rohan"),
        Row("",21,None),
        Row("",None,"Lily")
    ]
    schema = StructType([
        StructField("lines",StringType(), True),
        StructField("number",IntegerType(),True),
        StructField("name",StringType(),True)
    ])

    output_data = [
        Row("This is a demo 1st Row",24,"rohan"),
        Row("",21,"Unknown"),
        Row("",0,"Lily")
    ]

    input_df = spark.createDataFrame(input_data,schema)
    probable_output_df = spark.createDataFrame(output_data,schema)

    #Act
    actual_output_df = fillEmptyColumns(input_df,input_df.schema.names)

    

    #Assert
    try:
        #assert actual_output_df.schema == probable_output_df.schema
        assert actual_output_df.collect() == probable_output_df.collect()
        print("All test cases passed!!")

    except:
        print("Some issue occurred...")

test_fillEmptyColumn()

# COMMAND ----------



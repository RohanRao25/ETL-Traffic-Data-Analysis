# Databricks notebook source
# MAGIC %md
# MAGIC #Transforming Road Data in Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ###Creating Widget to intake the environment name

# COMMAND ----------

dbutils.widgets.text("Environment","","Enter the Environment name : ")
environment = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Running the common notebook to access the common variables and functions

# COMMAND ----------

# MAGIC %run "/Users/rohan.rao.2597@gmail.com/ETL-traffic-data-analysis/Notebook to Access External locations & Common Functions" $Environment = "dev"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Function to Read the Road data from bronze layer

# COMMAND ----------

def readRoadDataFromBronzeLayer(environment):

    print("Reading Road Data from Bronze Tables...")
    print("\n\n")

    df = spark.readStream.table(f"`{environment}-catalog-etl-traffic-data-analysis`.`bronze`.`raw_road`")

    print("********************************************Reading Road Data from Bronze Table complete!!******************************************")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformation Functions

# COMMAND ----------

from pyspark.sql.functions import when,col

# COMMAND ----------

# MAGIC %md
# MAGIC ######Adding column - road_category_name

# COMMAND ----------

def addColumnRoadCategory(df):

    print("Adding column - road_category_name...")
    print("\n\n")

    df_added_clumn_road_category = df.withColumn("road_category_name",when(col('Road_Category') == 'TA', 'Class A Trunk Road')
                  .when(col('Road_Category') == 'TM', 'Class A Trunk Motor')
                   .when(col('Road_Category') == 'PA','Class A Principal road')
                    .when(col('Road_Category') == 'PM','Class A Principal Motorway')
                    .when(col('Road_Category') == 'M','Class B road')
                    .otherwise('NA'))
    
    print("*****************************************Added column - road_category_name!!***************************************************")

    return df_added_clumn_road_category

# COMMAND ----------

# MAGIC %md
# MAGIC ######Adding column - road_type

# COMMAND ----------

def road_Type(df):
    print('Creating Road Type Name Column: ', end='')
    from pyspark.sql.functions import when,col

    df_road_Type = df.withColumn("Road_Type",
                  when(col('Road_Category_Name').like('%Class A%'),'Major')
                  .when(col('Road_Category_Name').like('%Class B%'),'Minor')
                    .otherwise('NA')
                  
                  )
    print('Success!! ')
    print('***********************')
    return df_road_Type

# COMMAND ----------

# MAGIC %md
# MAGIC ###Writing Transformed Data to Silver Layer Tables

# COMMAND ----------

def writeTransformedRoadDataToSilverTable(environment,df):

    print("Writing Transfomed Road Data to Silver Layer Tables...")
    print("\n\n")

    df.writeStream.format("delta").option("checkpointLocation",f"{checkpoint_external_location}RoadDataSilverCheckpoint/Checkpoint/").outputMode("append").queryName("writeRoadDataToSilverQuery").trigger(availableNow=True).toTable(f"`{environment}-catalog-etl-traffic-data-analysis`.`silver`.`transformed_road_data`")

    print("*************************************************Writing Transformed Road Data To Silver Layer Complete!!*****************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calling all functions

# COMMAND ----------

df = readRoadDataFromBronzeLayer(environment)

df_deduped = dropDuplicateRecords(df)

columns = df_deduped.schema.names
df_cleaned = fillEmptyColumns(df_deduped,columns)

df_added_road_cat_name = addColumnRoadCategory(df_cleaned)

df_added_road_type = road_Type(df_added_road_cat_name)

writeTransformedRoadDataToSilverTable(environment,df_added_road_type)

# COMMAND ----------



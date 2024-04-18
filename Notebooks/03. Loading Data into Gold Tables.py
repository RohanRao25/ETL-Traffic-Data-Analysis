# Databricks notebook source
# MAGIC %md
# MAGIC #Loading Data into Gold Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ###widget to intake environment name

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
# MAGIC ###Functions to read data from silver layer tables

# COMMAND ----------

# MAGIC %md
# MAGIC ######Reading Traffic Data

# COMMAND ----------

def readTrafficDataFromSilverLayer(environment):

    print("Reading Traffic Data from silver layer...")
    print("\n\n")

    df = spark.readStream.table(f"`{environment}-catalog-etl-traffic-data-analysis`.`silver`.`transformed_traffic_data`")

    print("*************************************Reading Traffic Data from Silver Layer complete!!*************************************************")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ######Reading Road Data

# COMMAND ----------

def readRoadDataFromSilverLayer(environment):

    print("Reading Road data from silver layer...")
    print("\n\n")

    df = spark.readStream.table(f"`{environment}-catalog-etl-traffic-data-analysis`.`silver`.`transformed_road_data`")

    print("***********************************Reading Road Data from Silver Layer complete!!***********************************************")

    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformation Functions

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ######Adding vehicle intensity to the traffic data

# COMMAND ----------

def addVehicleIntensityColumnTrafficData(df):

    print("Adding column - vehicle_intensity to traffic data...")
    print("\n\n")

    df_added_col = df.withColumn("vehicle_intensity", col("total_vehicles")/col("link_length_km"))

    print("*************************************Added column - vehicle_intensity to traffic data!!******************************************")

    return df_added_col

# COMMAND ----------

# MAGIC %md
# MAGIC ######Adding load time to traffic data

# COMMAND ----------

def addLoadTimeColumn(df):

    print("Adding column - load_time to traffic data...")
    print("\n\n")

    df_added_col = df.withColumn("load_time", current_timestamp())

    print("***********************************Added column - load_time to traffic data!!*****************************************************")

    return df_added_col

# COMMAND ----------

# MAGIC %md
# MAGIC ###Functions to write data to gold layer

# COMMAND ----------

# MAGIC %md
# MAGIC ######Writing Traffic Data to gold layer tables

# COMMAND ----------

def writeTrafficDataToGoldLayer(environment,df):

    print("Writing Traffic Data to Gold Layer...")
    print("\n\n")

    df.writeStream.format("delta").option("checkpointLocation",f"{checkpoint_external_location}trafficDataGoldCheckpoint/checkpoint/").outputMode("append").queryName("writeTrafficDataToGoldQuery").trigger(availableNow = True).toTable(f"`{environment}-catalog-etl-traffic-data-analysis`.`gold`.`final_traffic_data`")

    print("***************************************************Wrote Traffic Data to Gold Layer!!*************************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ######Writing Road Data to gold layer tables

# COMMAND ----------

def writeRoadDataToGoldLayer(environment,df):

    print("Writing Road Data to Gold Layer...")
    print("\n\n")

    df.writeStream.format("delta").option("checkpointLocation",f"{checkpoint_external_location}roadDataGoldCheckpoint/checkpoint/").outputMode("append").queryName("writeRoadDataToGoldQuery").trigger(availableNow=True).toTable(f"`{environment}-catalog-etl-traffic-data-analysis`.`gold`.`final_road_data`")

    print("**********************************************Wrote Road Data to Gold Layer!!******************************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calling the required Functions
# MAGIC
# MAGIC

# COMMAND ----------

df_traffic = readTrafficDataFromSilverLayer(environment)

df_traffic_deduped = dropDuplicateRecords(df_traffic)

df_traffic_cleaned = fillEmptyColumns(df_traffic_deduped, df_traffic_deduped.schema.names)

df_traffic_added_col_veh_intensity = addVehicleIntensityColumnTrafficData(df_traffic_cleaned)

df_traffic_added_col_load_time = addLoadTimeColumn(df_traffic_added_col_veh_intensity)

writeTrafficDataToGoldLayer(environment,df_traffic_added_col_load_time)

df_road = readRoadDataFromSilverLayer(environment)

df_road_deduped = dropDuplicateRecords(df_road)

df_road_cleaned = fillEmptyColumns(df_road_deduped, df_road_deduped.schema.names)

writeRoadDataToGoldLayer(environment,df_road_cleaned)

# COMMAND ----------



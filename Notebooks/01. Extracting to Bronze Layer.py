# Databricks notebook source
# MAGIC %md
# MAGIC #Extracting To Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ###Widget to recieve the environment details

# COMMAND ----------

dbutils.widgets.text("Environment","","Enter the environment name : ")
environment = dbutils.widgets.get("Environment")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Running the common notebook to access common variables and functions

# COMMAND ----------

# MAGIC %run "/Users/rohan.rao.2597@gmail.com/ETL-traffic-data-analysis/Notebook to Access External locations & Common Functions" $Environment="dev"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Functions for reading Data from landing zone

# COMMAND ----------

# MAGIC %md
# MAGIC ######Reading traffic data

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def readRawTrafficData():

    print("Reading Raw Traffic data from landing zone...")
    print("\n\n")

    schema = StructType([
    StructField("Record_ID",IntegerType()),
    StructField("Count_point_id",IntegerType()),
    StructField("Direction_of_travel",StringType()),
    StructField("Year",IntegerType()),
    StructField("Count_date",StringType()),
    StructField("hour",IntegerType()),
    StructField("Region_id",IntegerType()),
    StructField("Region_name",StringType()),
    StructField("Local_authority_name",StringType()),
    StructField("Road_name",StringType()),
    StructField("Road_Category_ID",IntegerType()),
    StructField("Start_junction_road_name",StringType()),
    StructField("End_junction_road_name",StringType()),
    StructField("Latitude",DoubleType()),
    StructField("Longitude",DoubleType()),
    StructField("Link_length_km",DoubleType()),
    StructField("Pedal_cycles",IntegerType()),
    StructField("Two_wheeled_motor_vehicles",IntegerType()),
    StructField("Cars_and_taxis",IntegerType()),
    StructField("Buses_and_coaches",IntegerType()),
    StructField("LGV_Type",IntegerType()),
    StructField("HGV_Type",IntegerType()),
    StructField("EV_Car",IntegerType()),
    StructField("EV_Bike",IntegerType())
    ])

    raw_traffic_df = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation",f"{checkpoint_external_location}schema/raw_traffic_schema").schema(schema).option("header",True).load(f"{landing_external_location}raw_traffic/").withColumn("Extract_Time",current_timestamp())
    

    print("******************************************Reading Raw Traffic data from landing zone complete!!***************************************")
    

    return raw_traffic_df


# COMMAND ----------

# MAGIC %md
# MAGIC ######Reading roads data

# COMMAND ----------

def readRawRoadData():

    print("Reading Raw Road data from landing zone...")
    print("\n\n")

    schema =  StructType([
        StructField('Road_ID',IntegerType()),
        StructField('Road_Category_Id',IntegerType()),
        StructField('Road_Category',StringType()),
        StructField('Region_ID',IntegerType()),
        StructField('Region_Name',StringType()),
        StructField('Total_Link_Length_Km',DoubleType()),
        StructField('Total_Link_Length_Miles',DoubleType()),
        StructField('All_Motor_Vehicles',DoubleType())
        
        ])
    
    df_raw_road = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation",f"{checkpoint_external_location}schema/raw_road_schema").option("header",True).schema(schema).load(f"{landing_external_location}/raw_road/")

    print("********************************************Reading Raw Road data from bronze layer complete!!*****************************************")

    return df_raw_road


# COMMAND ----------

# MAGIC %md
# MAGIC ###Function to write data to bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC ######Writing raw traffic data

# COMMAND ----------

def writeRawTrafficData(environment,df):

    print("Writing raw traffic data to bronze layer table...")
    print("\n\n")

    wwriteStreamQuery = df.writeStream.format("delta").option("checkpointLocation",f"{checkpoint_external_location}/checkpoint/raw_traffic_checkpoint").outputMode("append").trigger(availableNow=True).queryName("writeRawTrafficDataQuery").toTable(f"`{environment}-catalog-etl-traffic-data-analysis`.`bronze`.`raw_traffic`")
    

    print("************************************************Writing Raw Traffic Data to bronze table complete!!************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ######Writing raw road data

# COMMAND ----------

def writeRawRoadData(environment,df):

    print("Writing raw road data to bronze layer table...")
    print("\n\n")

    df.writeStream.format("delta").option("checkpointLocation",f"{checkpoint_external_location}checkpoint/raw_road_checkpoint").outputMode("append").queryName("writeRawRoadQuery").trigger(availableNow = True).toTable(f"`{environment}-catalog-etl-traffic-data-analysis`.`bronze`.`raw_road`")

    print("************************************************Writing Raw Traffic Data to bronze table complete!!************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calling the functions

# COMMAND ----------

df_raw_traffic = readRawTrafficData()
writeRawTrafficData(environment,df_raw_traffic)

df_raw_road = readRawRoadData()
writeRawRoadData(environment,df_raw_road)


# COMMAND ----------



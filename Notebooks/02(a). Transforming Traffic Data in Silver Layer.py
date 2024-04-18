# Databricks notebook source
# MAGIC %md
# MAGIC #Transforming Traffic Data in Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ###Widget to intake the Environment name

# COMMAND ----------

dbutils.widgets.text("Environment","","Enter the environment name : ")
environment = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Run the common notebook to access the common variables and functions

# COMMAND ----------

# MAGIC %run "/Users/rohan.rao.2597@gmail.com/ETL-traffic-data-analysis/Notebook to Access External locations & Common Functions" $Environment = "dev"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Functions to read data from bronze layer

# COMMAND ----------

def readTrafficDataFromBronzeLayer(environment):

    print("Reading Traffic Data from Bronze layer...")
    print("\n\n")

    df_traffic_data = spark.readStream.table(f"`{environment}-catalog-etl-traffic-data-analysis`.`bronze`.`raw_traffic`")

    print("*************************************Reading Traffic Data from bronze layer Complete!!********************************************")

    return df_traffic_data

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transformation Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ######Adding the column - Total EV vehicles

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

def addingColumnTotalEVVehicles(df):

    print("Adding Column - total_ev_vehicles...")
    print("\n\n")

    df_added_column = df.withColumn("total_ev_vehicles", col("ev_car") + col("ev_bike"))

    print("***************************************Added column - total_ev_vehicle!!**********************************************")
    
    return df_added_column

# COMMAND ----------

# MAGIC %md
# MAGIC ######Adding column - total_vehicles

# COMMAND ----------

def addingColumnTotalVehicles(df):

    print("Adding column. - total_vehicles...")
    print("\n\n")

    df_added_column = df.withColumn("total_vehicles", col("total_ev_vehicles") + col("Two_wheeled_motor_vehicles") + col("Cars_and_taxis") + col("Buses_and_coaches") + col("LGV_Type") + col("HGV_Type"))

    print("****************************************Added column - total_vehicle!!************************************************")

    return df_added_column

# COMMAND ----------

# MAGIC %md
# MAGIC ######Adding Column - Transformed_time

# COMMAND ----------

def addColumnTransformedtime(df):

    print("Adding column. - transformed_time...")
    print("\n\n")

    df_added_column = df.withColumn("transformed_time", current_timestamp())

    print("****************************************Added column - transformed_time!!************************************************")

    return df_added_column
    



# COMMAND ----------

# MAGIC %md
# MAGIC ###Function to write data to Silver layer

# COMMAND ----------

def writeTrafficDataToSilverTable(environment,df):

    print("Writing Transformed Traffic Data to silver layer...")
    print("\n\n")

    df.writeStream.format("delta").option("checkpointLocation",f"{checkpoint_external_location}trafficDataSilverCheckpoint/checkpoint/").outputMode("append").trigger(availableNow=True).queryName("WriteTrafficDataToSilverQuery").toTable(f"`{environment}-catalog-etl-traffic-data-analysis`.`silver`.`transformed_traffic_data`")

    print("*********************************************Wrote Traffic Data successfully to silver layer!!****************************************")



# COMMAND ----------

# MAGIC %md
# MAGIC ###Calling functions

# COMMAND ----------

df = readTrafficDataFromBronzeLayer(environment)

df_data_deduped = dropDuplicateRecords(df)

columns = df_data_deduped.schema.names

df_data_cleaned = fillEmptyColumns(df_data_deduped,columns)

df_added_column_total_ev = addingColumnTotalEVVehicles(df_data_cleaned)

df_added_column_total_vehicles = addingColumnTotalVehicles(df_added_column_total_ev)

df_added_column_transformed_time = addColumnTransformedtime(df_added_column_total_vehicles)

writeTrafficDataToSilverTable(environment,df_added_column_transformed_time)



# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC #One-time Script for creating required Schemas & Relevant Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ###Defining the widget to intake the environment name

# COMMAND ----------

dbutils.widgets.text("Environment","","Enter the environment name : ")
environment = dbutils.widgets.get("Environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Function to create the required schemas

# COMMAND ----------

def createRequiredSchema(environment, schema_name):

    print(f"Creating the schema - {schema_name}...")
    print("\n\n")
    print(f"'abfss://medallion-{environment}@{environment}stgaccnt.dfs.core.windows.net/{schema_name}'")

    spark.sql(f"""
              USE CATALOG '{environment}-catalog-etl-traffic-data-analysis'
              """)

    spark.sql(f"""
              CREATE SCHEMA IF NOT EXISTS `{schema_name}`
              MANAGED LOCATION 'abfss://medallion-{environment}@{environment}stgaccnt.dfs.core.windows.net/{schema_name}/'
              """)
    
    print("**********************************************Created the schema - {schema_name}!!***********************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Function to create the required tables in the bronze schema

# COMMAND ----------

# MAGIC %md
# MAGIC ######Creating 'raw_traffic' table

# COMMAND ----------

def createRawTrafficTable(environment):

  print("Creating 'raw_traffic' table in bronze schema...")
  print("\n\n")

  spark.sql(f"""
            CREATE TABLE IF NOT EXISTS `{environment}-catalog-etl-traffic-data-analysis`.`bronze`.`raw_traffic`(
              Record_ID INT,
                            Count_point_id INT,
                            Direction_of_travel VARCHAR(255),
                            Year INT,
                            Count_date VARCHAR(255),
                            hour INT,
                            Region_id INT,
                            Region_name VARCHAR(255),
                            Local_authority_name VARCHAR(255),
                            Road_name VARCHAR(255),
                            Road_Category_ID INT,
                            Start_junction_road_name VARCHAR(255),
                            End_junction_road_name VARCHAR(255),
                            Latitude DOUBLE,
                            Longitude DOUBLE,
                            Link_length_km DOUBLE,
                            Pedal_cycles INT,
                            Two_wheeled_motor_vehicles INT,
                            Cars_and_taxis INT,
                            Buses_and_coaches INT,
                            LGV_Type INT,
                            HGV_Type INT,
                            EV_Car INT,
                            EV_Bike INT,
                            Extract_Time TIMESTAMP

            )
            """)
  
  print("*********************************************Created table. - 'raw_traffic' in bronze schema!!****************************************")
  
  

# COMMAND ----------

# MAGIC %md
# MAGIC ######Create table 'raw_road' in bronze schema

# COMMAND ----------

def createRawRoadTable(environment):

    print("creating table 'raw_road' in bronze schema...")
    print("\n\n")

    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS `{environment}-catalog-etl-traffic-data-analysis`.`bronze`.`raw_road`(
                  Road_ID INT,
                            Road_Category_Id INT,
                            Road_Category VARCHAR(255),
                            Region_ID INT,
                            Region_Name VARCHAR(255),
                            Total_Link_Length_Km DOUBLE,
                            Total_Link_Length_Miles DOUBLE,
                            All_Motor_Vehicles DOUBLE
              )
              """)
    
    print("************************************************created table 'raw_road' in bronze schema*******************************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Calling the functions

# COMMAND ----------

createRequiredSchema(environment,"silver")
createRequiredSchema(environment,"bronze")
createRequiredSchema(environment,"gold")

createRawTrafficTable(environment)
createRawRoadTable(environment)

# COMMAND ----------



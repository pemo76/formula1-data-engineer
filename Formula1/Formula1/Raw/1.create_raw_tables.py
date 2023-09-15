# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create circuit table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF  EXISTS f1_raw.circuits;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.circuits(circuitId INT,
# MAGIC circuitRef STRING,
# MAGIC name STRING,
# MAGIC location STRING,
# MAGIC country STRING,
# MAGIC lat DOUBLE,
# MAGIC lng DOUBLE,
# MAGIC alt INT,
# MAGIC url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path '/mnt/formula1dlldatabrick/raw/circuits.csv',header true) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.circuits;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create races table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF  EXISTS f1_raw.races;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
# MAGIC year INT,
# MAGIC round INT,
# MAGIC circuitID INT,
# MAGIC name STRING,
# MAGIC date DATE,
# MAGIC time STRING,
# MAGIC url STRING
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path '/mnt/formula1dlldatabrick/raw/races.csv',header true) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.races;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create constructor table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF  EXISTS f1_raw.constructors;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors(
# MAGIC   constructorId INT,
# MAGIC   constructorRef STRING,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path '/mnt/formula1dlldatabrick/raw/constructors.json',header true) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.constructors;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Drivers Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF  EXISTS f1_raw.drivers;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.drivers(
# MAGIC   driverId INT,
# MAGIC   driverRef STRING,
# MAGIC   number INT,
# MAGIC   code STRING,
# MAGIC   name STRUCT<forename: STRING, surname: STRING>,
# MAGIC   dob DATE,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path '/mnt/formula1dlldatabrick/raw/drivers.json',header true) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.drivers;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Result table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF  EXISTS f1_raw.results;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.results(
# MAGIC raceId INT,
# MAGIC driverId INT,
# MAGIC constructorId INT,
# MAGIC number INT,
# MAGIC grid INT,
# MAGIC position INT,
# MAGIC positionText STRING,
# MAGIC positionOrder INT,
# MAGIC points FLOAT,
# MAGIC laps INT,
# MAGIC time STRING,
# MAGIC milliseconds INT,
# MAGIC fastestlap INT,
# MAGIC rank INT,
# MAGIC fastestLapTime STRING,
# MAGIC fastestLapSpeed FLOAT,
# MAGIC statusId STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path '/mnt/formula1dlldatabrick/raw/results.json',header true) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.results;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create pitstop table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF  EXISTS f1_raw.pit_stops;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
# MAGIC driverId INT,
# MAGIC duration STRING,
# MAGIC laps INT,
# MAGIC milliseconds INT,
# MAGIC raceId INT,
# MAGIC stop INT,
# MAGIC time STRING
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path '/mnt/formula1dlldatabrick/raw/pit_stops.json', multiLine true) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.pit_stops;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Lap Times Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF  EXISTS f1_raw.lap_times;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
# MAGIC raceId INT,
# MAGIC driverId INT,
# MAGIC lap INT,
# MAGIC position INT,
# MAGIC time STRING,
# MAGIC milliseconds INT
# MAGIC )
# MAGIC USING csv
# MAGIC OPTIONS (path '/mnt/formula1dlldatabrick/raw/lap_times') 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.lap_times;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Qualifying Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF  EXISTS f1_raw.qualifying;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
# MAGIC constructorId INT,
# MAGIC driverId INT,
# MAGIC number INT,
# MAGIC position INT,
# MAGIC q1 STRING,
# MAGIC q2 STRING,
# MAGIC q3 STRING,
# MAGIC qualifyId INT,
# MAGIC raceId INT
# MAGIC )
# MAGIC USING json
# MAGIC OPTIONS (path '/mnt/formula1dlldatabrick/raw/qualifying', multiLine true) 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.qualifying;
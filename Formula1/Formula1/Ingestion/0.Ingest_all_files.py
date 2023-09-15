# Databricks notebook source
v_result = dbutils.notebook.run("1.Ingest_circuits_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.Ingest_Races_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.Ingest_constructors_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.Ingest_Drivers_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.Ingest_Results_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.Ingest_Pitstops_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.Ingest_Lap_times_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.Ingest_qualifying_file",0,{"p_data_source": "Ergast API"})

# COMMAND ----------

v_result
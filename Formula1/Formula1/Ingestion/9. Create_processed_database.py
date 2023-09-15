# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_processed
# MAGIC LOCATION '/mnt/formula1dlldatabrick/processed'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE f1_processed;
# Databricks notebook source
# MAGIC %md # Workshop - Data Pipeline in Practice

# COMMAND ----------

# MAGIC %md ##### Read in Dataset

# COMMAND ----------

library(SparkR)
library(sparklyr)
library(dplyr)
library(lubridate)


# COMMAND ----------


SparkR::sparkR.session()


# COMMAND ----------

sc <- spark_connect(method = "databricks")


# COMMAND ----------



# COMMAND ----------

df_source_laptimes <- spark_read_csv(sc, name = "lap_times", path = "s3://columbia-gr5069-main/raw/lap_times.csv")

# COMMAND ----------

display(df_source_laptimes)

df_driver <- spark_read_csv(sc, name = "lap_times", path = "s3://columbia-gr5069-main/raw/drivers.csv")

# COMMAND ----------

(df_driver)

# COMMAND ----------


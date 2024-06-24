# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "67f8983e-c811-4672-9b76-77704bf6075a",
# META       "default_lakehouse_name": "SQLDW",
# META       "default_lakehouse_workspace_id": "9fee2690-4084-4127-9ba5-0ca1b1180451",
# META       "known_lakehouses": [
# META         {
# META           "id": "67f8983e-c811-4672-9b76-77704bf6075a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


%%sql

--DROP DATABASE IF EXISTS SCD CASCADE;
CREATE DATABASE IF NOT EXISTS SCD;


# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE SQLDW.SCD_DimDepartmentGroup

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import col, row_number, concat
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.functions import lit
# MAGIC 
# MAGIC #Reading_df = sqlContext.table("view_Columns_scd")
# MAGIC 
# MAGIC df = spark.read\
# MAGIC   .format("parquet")\
# MAGIC   .load("Files/Files/SCD/SCD_DimDepartmentGroup_RAW/*.parquet")
# MAGIC 
# MAGIC w = Window().orderBy("DepartmentGroupKey")
# MAGIC Reading_df_col = df.withColumn("From_date", F.current_date())\
# MAGIC                     .withColumn("ID_valid",lit("1")).withColumn("ID_Deleted",lit("0")).withColumn("End_date",  lit(None)).withColumn("ID_Surr", concat(row_number().over(w) + 1))
# MAGIC ##convert the column to datetime
# MAGIC Reading_df_col = Reading_df_col.withColumn("End_date",col("End_date").cast("date"))
# MAGIC                   
# MAGIC Reading_df_col.show(15)
# MAGIC 
# MAGIC Reading_df_col.write.format("delta").mode("overwrite").saveAsTable("SQLDW.SCD_DimDepartmentGroup")

# METADATA ********************

# META {
# META   "language": "python"
# META }

# CELL ********************

from pyspark.sql import functions as F

F.current_date() 

# CELL ********************

# MAGIC %%pyspark
# MAGIC 
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import col, row_number, concat
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.functions import lit
# MAGIC from pyspark.sql.functions import max
# MAGIC from pyspark.sql.functions import *
# MAGIC from delta.tables import *
# MAGIC 
# MAGIC 
# MAGIC  #REad the data
# MAGIC df_main = spark.read\
# MAGIC   .format('delta')\
# MAGIC   .table('SQLDW.SCD_DimDepartmentGroup')
# MAGIC 
# MAGIC 
# MAGIC df_silver = spark.read\
# MAGIC   .format('delta')\
# MAGIC   .load("Files/Files/SCD/SCD_DimDepartmentGroup_Silver")
# MAGIC 
# MAGIC df_silver= df_silver.fillna(0)
# MAGIC df_main= df_main.fillna(0)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ##got the max value
# MAGIC #const_surr = df_main.select(max(df_main.ID_Surr))+1
# MAGIC 
# MAGIC #max_id_surr = df_main.select(F.max(df_main.ID_Surr)).first()[0]
# MAGIC #const_surr = int(max_id_surr) + 1
# MAGIC 
# MAGIC 
# MAGIC ###only for this POC
# MAGIC df_silver = df_silver.drop("surrogate_ID")
# MAGIC df_silver = df_silver.drop("ID_valid")
# MAGIC df_silver = df_silver.drop("ID_Deleted")
# MAGIC df_silver = df_silver.drop("Curr_date")
# MAGIC df_silver=df_silver.dropDuplicates(['DepartmentGroupKey','ParentDepartmentGroupKey','DepartmentGroupName'])
# MAGIC 
# MAGIC 
# MAGIC ##Insert - Returns only columns from the left dataset for non-matched records.
# MAGIC df_left_forIns= df_silver.join(df_main,df_silver.DepartmentGroupKey == df_main.DepartmentGroupKey, "leftanti")
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ##adding the version  columns
# MAGIC df_left_forIns = df_left_forIns.withColumn("From_date", F.current_date())\
# MAGIC                    .withColumn("ID_valid",lit("1")).withColumn("ID_Deleted",lit("0")).withColumn("End_date",  lit(None))
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ##remove duplicated
# MAGIC df_left_forIns.dropDuplicates()
# MAGIC 
# MAGIC df_left_forIns.show()
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC #Everything that exist as the same in both dataframes
# MAGIC #first steps for the update
# MAGIC #using Spark SQL
# MAGIC df_main.createOrReplaceTempView("df_main_view")
# MAGIC df_silver.createOrReplaceTempView("df_silver_view")
# MAGIC df_left_forIns.createOrReplaceTempView("df_left_forIns_view")
# MAGIC 
# MAGIC df_join_forUpd=spark.sql("Select Main.DepartmentGroupKey " 
# MAGIC                          ",Main.ParentDepartmentGroupKey " 
# MAGIC                           ", Main.DepartmentGroupName " 
# MAGIC                           ", Main.ID_valid " 
# MAGIC                           ", Main.ID_Deleted " 
# MAGIC                           ", Main.From_date " 
# MAGIC                           ", Main.End_date  "
# MAGIC                           "From df_main_view Main Inner Join df_silver_view SILVER " 
# MAGIC                           "On SILVER.DepartmentGroupKey = Main.DepartmentGroupKey  "
# MAGIC                           "and SILVER.ParentDepartmentGroupKey = Main.ParentDepartmentGroupKey  " 
# MAGIC                           "and  SILVER.DepartmentGroupName = Main.DepartmentGroupName "
# MAGIC                           "Where not exists "
# MAGIC                             " (Select 1 From df_left_forIns_view Ins Where Ins.DepartmentGroupKey == Main.DepartmentGroupKey)")
# MAGIC df_join_forUpd.show()
# MAGIC 
# MAGIC 
# MAGIC ##adding the version  Surrogate
# MAGIC #df_join_forUpd = df_join_forUpd.withColumn("ID_Surr",  F.lit(int(const_surr)))
# MAGIC 
# MAGIC #everyhing from the df_main that do not exist on df_join_forUpd and df_left_forIns
# MAGIC df_anti_forUpd = df_main.join(df_join_forUpd,df_main.DepartmentGroupKey == df_join_forUpd.DepartmentGroupKey, "leftanti")
# MAGIC df_anti_forUpd.show()
# MAGIC 
# MAGIC #df_anti_forUpd_expired = df_main.join(df_anti_forUpd,df_main.DepartmentGroupKey == df_anti_forUpd.DepartmentGroupKey, "inner").distinct(df_main.DepartmentGroupKey)
# MAGIC #df_anti_forUpd_expired.show()
# MAGIC 
# MAGIC #first make a copy
# MAGIC df_anti_forUpd_copy = df_anti_forUpd
# MAGIC #now drop the columns for the Union
# MAGIC df_anti_forUpd = df_anti_forUpd.drop("ID_Surr")
# MAGIC 
# MAGIC 
# MAGIC ##Union the dataframes. Now we can merge and manage  the versions
# MAGIC unionDF = df_anti_forUpd.union(df_left_forIns)
# MAGIC 
# MAGIC 
# MAGIC # Add increasing ID column
# MAGIC max_id_surr = df_main.select(F.max(df_main.ID_Surr)).first()[0]
# MAGIC max_surr = int(max_id_surr) + 1
# MAGIC 
# MAGIC # Add increasing ID column
# MAGIC windowSpec = Window().orderBy("DepartmentGroupKey")
# MAGIC unionDF = unionDF.withColumn("ID_Surr", max_surr + row_number().over(windowSpec) - 1)
# MAGIC 
# MAGIC unionDF.show(10)
# MAGIC df_anti_forUpd_copy.show(10)
# MAGIC 
# MAGIC unionDF_complete = unionDF.union(df_anti_forUpd_copy)
# MAGIC unionDF_complete.show(10)
# MAGIC 
# MAGIC 
# MAGIC deltaTableNew = DeltaTable.forName(spark, 'SQLDW.SCD_DimDepartmentGroup')
# MAGIC 
# MAGIC 
# MAGIC deltaTableNew.alias('SCD_DimDepartmentGroup') \
# MAGIC   .merge(
# MAGIC     unionDF_complete.alias('updates'),
# MAGIC     'SCD_DimDepartmentGroup.ID_Surr = updates.ID_Surr' 
# MAGIC   ) \
# MAGIC   .whenMatchedUpdate(set =
# MAGIC     {
# MAGIC         "ID_valid": "0",
# MAGIC         "ID_Deleted" : "1",
# MAGIC         "End_date" : current_date() 
# MAGIC     }
# MAGIC   ) \
# MAGIC   .whenNotMatchedInsert(values =
# MAGIC     {
# MAGIC       "DepartmentGroupKey" :"updates.DepartmentGroupKey",
# MAGIC       "ParentDepartmentGroupKey" :"updates.ParentDepartmentGroupKey",
# MAGIC       "DepartmentGroupName" :"updates.DepartmentGroupName",
# MAGIC       "ID_Surr":"ID_Surr",
# MAGIC       "ID_valid" : "1",
# MAGIC       "ID_Deleted": "0",
# MAGIC       "From_date" : current_date()
# MAGIC      }
# MAGIC   ) \
# MAGIC   .execute()
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC #dfUpdates.Show()
# MAGIC #deltaTableNew.show()
# MAGIC 
# MAGIC #Read the data updated
# MAGIC #union with the data versioned with the new row for the update
# MAGIC #overwrite.
# MAGIC 
# MAGIC #df_main = spark.read\
# MAGIC  # .format('delta')\
# MAGIC   #.table('SCD.SCD_DimDepartmentGroup')
# MAGIC 
# MAGIC #df_main =  df_main.union(df_anti_forUpd)
# MAGIC 
# MAGIC ##renamed
# MAGIC #df_main =  df_main.withColumnRenamed("ID_Surr", "ID_Surr_old")
# MAGIC 
# MAGIC ##recreate column and order again
# MAGIC #w =  Window().orderBy("ID_Surr_old")
# MAGIC #df_main =df_main.withColumn("ID_Surr", concat(row_number().over(w) + 1))
# MAGIC 
# MAGIC ##drop column
# MAGIC #df_main =  df_main.drop( "ID_Surr_old")
# MAGIC 
# MAGIC 
# MAGIC #df_main.write.format("delta").mode("overwrite").saveAsTable("SCD.SCD_DimDepartmentGroup")
# MAGIC 
# MAGIC df_main.show (10)

# METADATA ********************

# META {
# META   "language": "python"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SQLDW.scd_dimdepartmentgroup LIMIT 1000")
display(df)

# CELL ********************

# MAGIC %%sql
# MAGIC OPTIMIZE SQLDW.SCD_DimDepartmentGroup

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM SQLDW.scd_dimdepartmentgroup LIMIT 1000")
display(df)

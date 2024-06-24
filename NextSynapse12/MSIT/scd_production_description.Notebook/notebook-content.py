# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "93b443f5-777a-4196-9e74-d4aea2ca4700",
# META       "default_lakehouse_name": "SQLDB_Synapse",
# META       "default_lakehouse_workspace_id": "e39e052f-46ae-4759-91bd-c810a117a436",
# META       "known_lakehouses": [
# META         {
# META           "id": "93b443f5-777a-4196-9e74-d4aea2ca4700"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### First Load of SCD

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
# MAGIC   .load("Files/Raw/SQLDB/Tables/Production.ProductDescription/*.parquet")
# MAGIC 
# MAGIC w = Window().orderBy("ProductDescriptionID")
# MAGIC Reading_df_col = df.withColumn("From_date", F.current_date())\
# MAGIC                     .withColumn("ID_valid",lit("1"))\
# MAGIC                     .withColumn("ID_Deleted",lit("0"))\
# MAGIC                     .withColumn("End_date",  lit(None))\
# MAGIC                     .withColumn("ID_Surr", concat(row_number().over(w) + 1))
# MAGIC ##convert the column to datetime
# MAGIC Reading_df_col = Reading_df_col.withColumn("End_date",col("End_date").cast("date"))
# MAGIC                   
# MAGIC Reading_df_col.show(15)
# MAGIC 
# MAGIC Reading_df_col.write.format("delta").mode("overwrite").saveAsTable("ProductDescription_WithSCD")

# METADATA ********************

# META {
# META   "language": "python"
# META }

# MARKDOWN ********************

# #### Copy from raw to silver

# CELL ********************

#Copy the raw to silver zone
#mssparkutils.fs.cp('abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/Production.ProductDescription/*.parquet','abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Silver/SQLDB/Tables/Production.ProductDescription/*.parquet' )

#df = "abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Raw/SQLDB/Tables/Production.ProductDescription/"

#print("Remote blob path: " + df)

source="Files/Raw/SQLDB/Tables/Production.ProductDescription/"
destiny ='Files/Silver/'
folder_destination =  'SQLDB/Tables/Production.ProductDescription/'

def Copyfile(source,destiny,folder_destination):
    files = mssparkutils.fs.ls(source)
    for file in files:
        #print(file.name, file.isDir, file.isFile, file.path, file.size)
        mssparkutils.fs.cp(file.path,destiny+folder_destination+ file.name )

Copyfile (source,destiny,folder_destination)

# MARKDOWN ********************

# #### Files are in parquet format. Converting to Delta at the Silver Zone

# CELL ********************

# MAGIC %%sql
# MAGIC CONVERT TO DELTA parquet.`abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Silver/SQLDB/Tables/Production.ProductDescription/`;

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

# MAGIC %%pyspark
# MAGIC ##Whoops Data was changed accidentaly
# MAGIC 
# MAGIC from delta.tables import *
# MAGIC from pyspark.sql.functions import *
# MAGIC 
# MAGIC 
# MAGIC deltaTable = DeltaTable.forPath(spark, "/sqlserverlessanalitics/DimSalesReason_NONPart/")
# MAGIC  
# MAGIC 
# MAGIC # Update the table (reduce price of accessories by 10%)
# MAGIC deltaTable.update(
# MAGIC     condition = "[ModifiedDate] == '2013-04-30 00:00:00.000'",
# MAGIC     set = { "SalesReasonName": "'Demo EventX'" }
# MAGIC                 )

# METADATA ********************

# META {
# META   "language": "python"
# META }

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
# MAGIC def fillna_str(df,colname):
# MAGIC     df = df.fillna("No Values/Unknow", subset=[colname])
# MAGIC 
# MAGIC 
# MAGIC  #REad the data from main
# MAGIC df_main = spark.read\
# MAGIC   .format('delta')\
# MAGIC   .table('ProductDescription_WithSCD')
# MAGIC 
# MAGIC df_silver = spark.read\
# MAGIC   .format("delta")\
# MAGIC   .load("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Silver/SQLDB/Tables/Production.ProductDescription/")
# MAGIC 
# MAGIC 
# MAGIC #df_silver = spark.read\
# MAGIC  #   .parquet("abfss://fabricsynapse12@msit-onelake.dfs.fabric.microsoft.com/SQLDB_Synapse.Lakehouse/Files/Silver/SQLDB/Tables/Production.ProductDescription/*.parquet")
# MAGIC 
# MAGIC 
# MAGIC fillna_str(df_silver,"Description")
# MAGIC fillna_str(df_main,"Description")
# MAGIC 
# MAGIC 
# MAGIC ##Insert - Returns only columns from the left dataset for non-matched records.
# MAGIC df_left_forIns= df_silver.join(df_main,df_silver.ProductDescriptionID == df_main.ProductDescriptionID, "leftanti")
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ##adding the version  columns
# MAGIC df_left_forIns = df_left_forIns.withColumn("From_date", F.current_date())\
# MAGIC                    .withColumn("ID_valid",lit("1"))\
# MAGIC                    .withColumn("ID_Deleted",lit("0"))\
# MAGIC                    .withColumn("End_date",  lit(None))
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
# MAGIC df_join_forUpd=spark.sql("Select Main.ProductDescriptionID " 
# MAGIC                           ", Main.Description " 
# MAGIC                           ", Main.ModifiedDate " 
# MAGIC                           ", Main.ID_valid " 
# MAGIC                           ", Main.ID_Deleted " 
# MAGIC                           ", Main.From_date " 
# MAGIC                           ", Main.End_date  "
# MAGIC                           "From df_main_view Main Inner Join df_silver_view SILVER " 
# MAGIC                           "On SILVER.ProductDescriptionID = Main.ProductDescriptionID  "
# MAGIC                           "and SILVER.Description = Main.Description  " 
# MAGIC                           "and  SILVER.ModifiedDate = Main.ModifiedDate "
# MAGIC                           "Where not exists "
# MAGIC                             " (Select 1 From df_left_forIns_view Ins Where Ins.ProductDescriptionID == Main.ProductDescriptionID)")
# MAGIC df_join_forUpd.show()
# MAGIC 
# MAGIC 
# MAGIC ##adding the version  Surrogate
# MAGIC #df_join_forUpd = df_join_forUpd.withColumn("ID_Surr",  F.lit(int(const_surr)))
# MAGIC 
# MAGIC #everyhing from the df_main that do not exist on df_join_forUpd and df_left_forIns
# MAGIC df_anti_forUpd = df_main.join(df_join_forUpd,df_main.ProductDescriptionID == df_join_forUpd.ProductDescriptionID, "leftanti")
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
# MAGIC windowSpec = Window().orderBy("ProductDescriptionID")
# MAGIC unionDF = unionDF.withColumn("ID_Surr", max_surr + row_number().over(windowSpec) - 1)
# MAGIC 
# MAGIC unionDF.show(10)
# MAGIC df_anti_forUpd_copy.show(10)
# MAGIC 
# MAGIC unionDF_complete = unionDF.union(df_anti_forUpd_copy)
# MAGIC unionDF_complete.show(10)
# MAGIC 
# MAGIC 
# MAGIC deltaTableNew = DeltaTable.forName(spark, 'ProductDescription_WithSCD')
# MAGIC 
# MAGIC 
# MAGIC deltaTableNew.alias('ProductDescription_WithSCD') \
# MAGIC   .merge(
# MAGIC     unionDF_complete.alias('updates'),
# MAGIC     'ProductDescription_WithSCD.ID_Surr = updates.ID_Surr' 
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
# MAGIC       "ProductDescriptionID" :"updates.ProductDescriptionID",
# MAGIC       "Description" :"updates.Description",
# MAGIC       "ModifiedDate" :"updates.ModifiedDate",
# MAGIC       "ID_Surr":"ID_Surr",
# MAGIC       "ID_valid" : "1",
# MAGIC       "ID_Deleted": "0",
# MAGIC       "From_date" : current_date()
# MAGIC      }
# MAGIC   ) \
# MAGIC   .execute()
# MAGIC 
# MAGIC 
# MAGIC df_main.show (10)
# MAGIC 
# MAGIC df_main.write.format("delta").mode("overwrite").saveAsTable("ProductDescription_WithSCD")

# METADATA ********************

# META {
# META   "language": "python"
# META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "67f8983e-c811-4672-9b76-77704bf6075a",
# META       "default_lakehouse_name": "SQLDW",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "67f8983e-c811-4672-9b76-77704bf6075a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

print(parameterString)
#mssparkutils.notebook.exit("/SQLDB/Tables/" + str(input))
#fWritetableread = spark.sql("select * from " + str(input) )
#dfWritetableread.write.mode("overwrite").parquet("/SQLDB/Tables/" + str(input) )

    
    
#set variable to be used to connect the database
database = "AdventureWorks2017"
table = parameterString
user = "testOwner"
password  = "Contoso!0000"
    
    #print(nametable.value )

jdbcDF = spark.read \
    .format("jdbc") \
    .option("url",  f"jdbc:sqlserver://sqldbfta.database.windows.net:1433; database=AdventureWorks2017") \
    .option("dbtable", table) \
    .option("user", user) \
   .option("password", password).load()

jdbcDF.write.mode("overwrite").parquet("Files/Files/SQLDB_intrc/Tables/" + parameterString )



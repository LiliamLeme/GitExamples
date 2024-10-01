-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "30489185-60ec-4166-9f67-8df9d352b1af",
-- META       "default_lakehouse_name": "testdata",
-- META       "default_lakehouse_workspace_id": "45ed8171-d058-4807-b868-42c1ac3d3199",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "30489185-60ec-4166-9f67-8df9d352b1af"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************


Create table houseprice AS
SELECT       _c1 as value_
            ,_c2 as date_
            ,_c3 as postcode
            ,_c7 as address_1
            ,_c8 as number_
            ,_c9 as address_2
            ,_c11 as city
            ,_c12 as council
            ,_c13 as city_2
            ,'lilem@microsoft.com' as access_id ---External user
FROM ppukversion
where _c3='NW2 5BU'
Union 
SELECT       _c1 as value_
            ,_c2 as date_
            ,_c3 as postcode
            ,_c7 as address_1
            ,_c8 as number_
            ,_c9 as address_2
            ,_c11 as city
            ,_c12 as council
            ,_c13 as city_2
            ,'liliam.leme@MngEnvMCAP040685.onmicrosoft.com' as access_id --internal user
FROM ppukversion
where _c3<>'NW2 5BU'


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
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

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
#https://supportability.visualstudio.com/AzureSynapseAnalytics/_wiki/wikis/AzureSynapseAnalytics/577433/How-to-run-notebook-concurrently
#https://docs.python.org/3/library/concurrent.futures.html
#json_str
import json
notebooks = '''[{"path":"/Notebook_interactive", "parameterString": {"parameterString":"EMPTY_TABLE"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.EmployeeDepartmentHistory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.EmployeePayHistory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.JobCandidate"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.Shift"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.Address"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.AddressType"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.BusinessEntity"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.BusinessEntityAddress"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.BusinessEntityContact"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.ContactType"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.CountryRegion"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.EmailAddress"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.Password"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"dbo.information_schema_tables"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.Person"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.PersonPhone"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.PhoneNumberType"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.StateProvince"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.BillOfMaterials"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.Culture"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.Document"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.Illustration"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.Location"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.Product"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductCategory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductCostHistory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductDescription"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductDocument"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductInventory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductListPriceHistory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductModel"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductModelIllustration"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductModelProductDescriptionCulture"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductPhoto"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductProductPhoto"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductReview"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.vEmployee"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.vEmployeeDepartment"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ProductSubcategory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.vEmployeeDepartmentHistory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.vJobCandidate"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.ScrapReason"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.vJobCandidateEducation"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.vJobCandidateEmployment"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.TransactionHistory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.vAdditionalContactInfo"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Person.vStateProvinceCountryRegion"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.TransactionHistoryArchive"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.vProductAndDescription"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.vProductModelCatalogDescription"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.UnitMeasure"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.vProductModelInstructions"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Purchasing.vVendorWithAddresses"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.WorkOrder"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Purchasing.vVendorWithContacts"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.vPersonDemographics"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Production.WorkOrderRouting"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.vSalesPerson"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.vSalesPersonSalesByFiscalYears"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Purchasing.ProductVendor"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.vStoreWithAddresses"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.vStoreWithContacts"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Purchasing.PurchaseOrderDetail"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.vStoreWithDemographics"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Purchasing.PurchaseOrderHeader"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.Customer"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Purchasing.ShipMethod"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Purchasing.Vendor"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.vIndividualCustomer"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.CountryRegionCurrency"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.CreditCard"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.Currency"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.CurrencyRate"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.PersonCreditCard"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SalesOrderDetail"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SalesOrderHeader"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SalesOrderHeaderSalesReason"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SalesPerson"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SalesPersonQuotaHistory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SalesReason"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"dbo.AWBuildVersion"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SalesTaxRate"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"dbo.DatabaseLog"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SalesTerritory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"dbo.ErrorLog"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SalesTerritoryHistory"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"dbo.forTest"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.ShoppingCartItem"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"dbo.IndexKeysDemo"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"dbo.TEST_CONTEXT"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SpecialOffer"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"dbo.TestBitFirst"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.Department"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.SpecialOfferProduct"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"HumanResources.Employee"}},
{"path": "/Notebook_interactive", "parameterString": {"parameterString":"Sales.Store"}},
{"path":"/Notebook_interactive", "parameterString": {"parameterString":"EMPTY_TABLE"}}]'''

#%run Notebook {"MyParam":0.}

#print (notebooks)
timeout = 3600
inputJson = json.loads(notebooks)

#print(inputJson) 
with ThreadPoolExecutor() as ec:
  [ec.submit(mssparkutils.notebook.run, notebook["path"], 1200, notebook["parameterString"]) for notebook in inputJson]

#https://cloudarchitected.com/2019/05/running-azure-databricks-notebooks-in-parallel/
#https://www.geeksforgeeks.org/how-to-use-threadpoolexecutor-in-python3/


##5 min on Next, ##2min on Synapse..??? 


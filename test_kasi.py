import json
import uuid
import pymssql
import sys
import psycopg2
from ApiFunctions import *
from env import *

sourceConnectionId = "6f40c6b4-1c59-4601-9211-71b70f0e0513"
projectId = '3d5634f3-c5be-4101-877f-1a4b4df4e918'
cdcType=0



projectId= "3d5634f3-c5be-4101-877f-1a4b4df4e918"
sourceConnectionId= "80126575-8fea-49c2-af30-2560b83a2b26"
sourceName1= "[dbo].[TARPROMO]"
tableName1= "[dbo].[TARPROMO_all]"
tableNameHis1= "]"
columns1= [
{
  "id": "02141dd8-2c5f-4600-a50f-e5f23f093619",
  "code": "PAFORF",
  "dataType": "numeric",
  "isPrimaryKey": True
},
{
  "id": "091e54c9-6f28-4d1a-bff0-635d213e629b",
  "code": "PROMOTACOD",
  "dataType": "char",
  "isPrimaryKey": True
},
{
  "id": "09de1edd-0b89-4ec9-abe9-35738028c614",
  "code": "CE8",
  "dataType": "char",
  "isPrimaryKey": False
}
]



token = getToken(api_domain, api_user, api_password, api_tenantId)
WizardTableData = wizardGetTables(api_domain, api_tenantId, token, sourceConnectionId)
#print(WizardTableData)

tableName = "[dbo].[TARPROMO]"

for tableData in WizardTableData:
    if tableName.upper() in tableData['code'].upper():
        print("Entereed If")
        tableId = tableData['id']
        print (tableId)
        tableCode = tableData['code']


        WizardColumData = wizardGetColumns(api_domain, api_tenantId, token, tableId)
        print(WizardColumData)

        
        data = createWizardCdc(api_domain, api_tenantId, token, projectId, sourceConnectionId, cdcType, sourceName1, tableName1, tableNameHis1,
                               WizardColumnData, True, True, False)
        print(data)
        break
    

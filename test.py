import requests
import json
import uuid
import pymssql
import sys
import psycopg2
from ApiFunctions import *
from env import *

i=1


#MSSQL CONNEXION

try:
    mssql_conn = pymssql.connect(server=dwh_server, user=dwh_user,
                                 password=dwh_password, database=dwh_database)
except:
    print("Connexion à MSSQL impossible")
    sys.exit(1)


#DATA CONFIG
with open("C:/Users/ADS12/Desktop/wrk_test.txt", "r") as file:
  content = file.read()

data = json.loads(content)

for items in data:
  lst = []
  for key, value in items.items():
    lst.append(value)

#GetToken
token=getToken(api_domain,api_user,api_password,api_tenantId)

#CreateWorkflow
workflowId=createWorkflow(api_domain,nomObjet,projectId,api_tenantId,token)


#MSSQL QUERY getProject
mssql_cursor = mssql_conn.cursor()
mssql_cursor.execute(getProject.format(nomFluxNext))
rows = mssql_cursor.fetchall()


for row in rows:

    #CREATE DATA PIPELINE
    flowId=createDataPipeline(api_domain,projectId,row[2],destinationConnectionId,1,1,nomFluxNext[6:],"[dbo].[" +row[0]+"]",row[1],token,api_tenantId)

    #MSSQL QUERY getFlowColumns
    mssql_cursor2 = mssql_conn.cursor()
    mssql_cursor2.execute(getFlowColumns.format(nomFluxNext,row[0],row[3]))
    rows2 = mssql_cursor2.fetchall()

    for row2 in rows2:
        #CREATE FLOW COLUMNS
        addFlowColumn(api_domain, flowId, row2[0], token, api_tenantId)

    #ADD WORKFLOW STEP
    addWorkflowStep(api_domain, 2, flowId, i, workflowId, api_tenantId, token)
    i+=1

    #GET WORKFLOW COLUMN
    flowColumnData=GetFlowColumn(api_domain, flowId, api_tenantId, token)

    for item in flowColumnData:
        if "NVARCHAR" in str(item["destinationColumnDataType"]):
            val = (item["id"], item["oFlowId"], item["destinationColumnDataType"].replace("NVARCHAR", "VARCHAR"),
                   item["sourceColumnDatatype"], item["sourceColumnName"], item["destinationColumnName"])
            UpdateFlowColumn(api_domain,val[1],val[4],val[5],val[2],val[3],val[0],api_tenantId,token)

mssql_conn.close()
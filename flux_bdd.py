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

#GetToken
token=getToken(api_domain,api_user,api_password,api_tenantId)


#MSSQL QUERY getProjectLib
mssql_cursor = mssql_conn.cursor()
mssql_cursor.execute(getProjectLib)
rows3 = mssql_cursor.fetchall()


for row3 in rows3:
    i=1
    # MSSQL QUERY getProject
    mssql_cursor = mssql_conn.cursor()
    mssql_cursor.execute(getProject.format(row3[0]))
    rows = mssql_cursor.fetchall()
    #createProject
    print("\n*************************************************")
    projectId = createProject(api_domain, row3[0], organisationUnitId, api_tenantId, token)
    print("*************************************************\n")
    #createWorkflow
    workflowId = createWorkflow(api_domain, row3[0][6:], projectId, api_tenantId, token)

    for row in rows:

        #CREATE DATA PIPELINE
        flowId=createDataPipeline(api_domain,projectId,row[2],destinationConnectionId,1,1,row[0]+'_to_'+row[1],"[dbo].[" +row[0]+"]",row[1],token,api_tenantId)

        #MSSQL QUERY getFlowColumns
        mssql_cursor2 = mssql_conn.cursor()
        mssql_cursor2.execute(getFlowColumns.format(row3[0],row[0],row[3]))
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

    if row3[1] is None:
      continue
    elif len(row3[1]) == 0:
        continue
    else:

        #createSqlScript
        scriptId = createSqlScript(api_domain, row3[0][6:], row3[1], projectId,
                                   destinationConnectionId, api_tenantId, token)

        # ADD WORKFLOW STEP SCRIPT SQL
        addWorkflowStep(api_domain, 3, scriptId, i, workflowId, api_tenantId, token)

mssql_conn.close()
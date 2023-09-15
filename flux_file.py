import shutil
import pandas as pd
import sqlalchemy
import pymssql
import sys
from ApiFunctions import *
from env import *

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
mssql_cursor.execute(getProjectFile)
rows = mssql_cursor.fetchall()

for row in rows:
    print("\n*************************************************")
    projectId = createProject(api_domain, row[0], organisationUnitId, api_tenantId, token)
    print("*************************************************\n")
    #createWorkflow
    workflowId = createWorkflow(api_domain, row[0][6:], projectId, api_tenantId, token)

    if row[1] is None:
      continue
    elif len(row[1]) == 0:
        continue
    else:
        scriptId = createSqlScript(api_domain, row[0][6:], row[1], projectId,
                               destinationConnectionId, api_tenantId, token)

        addWorkflowStep(api_domain, 3, scriptId, 2, workflowId, api_tenantId, token)


mssql_conn.close()



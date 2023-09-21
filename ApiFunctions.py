import json
import requests

def getToken(domaine : str, username : str, pwd: str, tenantId : str) -> str:
    """
    --------------------------------------------------------------------------------------------------------------------
    getToken(domaine : str, username : str, pwd: str, tenantId : str) -> requests.models.Response

    Se connecte à l'api pour récupérer un token. Le token est essentiel pour appeler les autres API

    Args :
        - domaine (str) : exemple ; "https://monAPI.com"
        - username (str) : nom d'utilisateur pour s'identifier à l'api
        - pwd (str) : mot de passe pour s'identifier à l'api
        - tenantId : numéro du tenant sur lequel on souhaite se connecter. Exemple; 2,3,4

    Returns :
        Response : .text sur l'objet retourné pour récupérer le détail de l'exécution
    --------------------------------------------------------------------------------------------------------------------
    """
    try:
        # Vérification des types
        if not isinstance(domaine, str):
            raise Exception(f"\nErreur type de données  : \n 'Domaine' doit être un 'str', '{type(domaine)}' reçu")
        if not isinstance(username, str):
            raise Exception(f"\nErreur type de données  : \n 'username' doit être un 'str', '{type(username)}' reçu")
        if not isinstance(pwd, str):
            raise Exception(f"\nErreur type de données  : \n 'pwd' doit être un 'str', '{type(pwd)}' reçu")
        if not isinstance(tenantId, str):
            raise Exception(f"\nErreur type de données  : \n 'tenantId' doit être un 'str', '{type(tenantId)}' reçu")

        url = domaine + "/api/TokenAuth/Authenticate"

        payload = json.dumps({
            "userNameOrEmailAddress": username,
            "password": pwd

        })
        headers = {
            'Abp.TenantId': tenantId,
            'Accept': 'text/plain',
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        if response.status_code!=200:
            raise Exception(f"\n Connection failed : \n Statut : {response.status_code} \n Error message : {response.json()['error']['message']} \n Error detail : {response.text}")

        else:
            print("Accessed bearer token and connexion successfull")
            token = str(response.json()['result']['accessToken'])
            return token

    except TypeError as e:
        print(e)


def createWorkflow(domaine,nomObjet,projectId,tenantId,token,isScheduled=False,emailSentOnError=False,enableCrossProject=False):
  """
  ----------------------------------------------------------------------------------------------------------------------
  CREATE WORKFLOW
  ----------------------------------------------------------------------------------------------------------------------
  """
  url = domaine + "/api/services/app/OWorkflows/CreateOrEdit"

  payload = json.dumps({
      "name": nomObjet,
      "oProjectId": projectId,
      "isScheduled": isScheduled,
      "emailSentOnError": emailSentOnError,
      "enableCrossProject": enableCrossProject
  })
  headers = {
      'Abp.TenantId': tenantId,
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
  }

  try:
      response = requests.request("POST", url, headers=headers, data=payload)
      workflowId = str(response.json()['result'])
      print(f"Workflow '{nomObjet}' successfully created")
      return workflowId
  except requests.exceptions.RequestException as e:

      raise ValueError("An error occurred in GET TOKEN:\n" + str(e))


def createDataPipeline(domaine,projectId,sourceConnexionId,destinationConnexionId, sourceConnectionType, destinationConnectionType, nomObjet,sourceTable,destinationTable,token,tenantId):
      """
      ----------------------------------------------------------------------------------------------------------------------
      CREATE DATA PIPELINE
      ----------------------------------------------------------------------------------------------------------------------
      """

      url = domaine + "/api/services/app/OProjects/AddFlow"

      payload = json.dumps({
          "oProjectId": projectId,
          "sourceConnectionId": sourceConnexionId,
          "sourceConnectionType": sourceConnectionType,
          "destinationConnectionId": destinationConnexionId,
          "destinationConnectionType": destinationConnectionType,
          "name": nomObjet,
          "sourceTable": sourceTable,
          "destinationTable": destinationTable,
          "nbColumns": 0,
          "isScheduled": False,
          "isInAWorkflow": False,
          "actionIfTableExists": 0,
          "actionIfTableNotExists": 0,
          "desactivateIndexes": True,
          "createPrimaryKey": False,
          "createIndexes": False,
          "numberOfSchedules": 0
      })
      headers = {
          'Authorization': 'Bearer ' + token,
          'Abp.TenantId': tenantId,
          'Content-Type': 'application/json'
      }
      try:
        response = requests.request("POST", url, headers=headers, data=payload)
        flowId = str(response.json()['result'])
        print(f"Data pipeline '{nomObjet}' successfully created")
        return flowId

      except requests.exceptions.RequestException as e:

        raise ValueError("An error occurred in GET TOKEN:\n" + str(e))


def addFlowColumn(domaine, flowId, sourceColumnName, token, tenantId):
    """
    ----------------------------------------------------------------------------------------------------------------------
    ADD FLOW COLUMN
    ----------------------------------------------------------------------------------------------------------------------
    """
    url = domaine + "/api/services/app/OProjects/AddFlowColumn"

    payload = json.dumps({
          "oFlowId": flowId,
          "sourceColumnName": sourceColumnName,
          "missingInSource": False
      })
    headers = {
          'Authorization': 'Bearer ' + token,
          'Abp.TenantId': tenantId,
          'Content-Type': 'application/json'
      }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)

        print(f"Column '{sourceColumnName}' successfully added")

    except requests.exceptions.RequestException as e:

        raise ValueError("An error occurred in GET TOKEN:\n" + str(e))


def addWorkflowStep(domaine,jobType,flowId,stepOrder,workflowId,tenantId,token):
      """
      ----------------------------------------------------------------------------------------------------------------------
      ADD WORKFLOW STEP
      ----------------------------------------------------------------------------------------------------------------------
      """
      # DATA PIPELINE
      url = domaine + "/api/services/app/OWorkflows/CreateOrEditStep"

      payload = json.dumps({
          "jobType": jobType,
          "objectId": flowId,
          "stepOrder": stepOrder,
          "oWorkflowId": workflowId,
          "isActive": True,
          "stopWorkflowOnError": True
      })
      headers = {
          'Abp.TenantId': tenantId,
          'Authorization': 'Bearer ' + token,
          'Content-Type': 'application/json'
      }
      try:
          response = requests.request("POST", url, headers=headers, data=payload)

          print(f"Step '{stepOrder}' successfully added")

      except requests.exceptions.RequestException as e:

          raise ValueError("An error occurred in GET TOKEN:\n" + str(e))

def GetFlowColumn(domaine,flowId,tenantId,token):
      """
      ----------------------------------------------------------------------------------------------------------------------
      GET FLOW COLUMN
      ----------------------------------------------------------------------------------------------------------------------
      """
      url = domaine + "/api/services/app/OProjects/GetFlowColumns"

      params = {
          "input": flowId,  # Replace with your actual UUID value
          "included": "true",
      }

      headers = {
          "Abp.TenantId": tenantId,
          "Authorization": "Bearer " + token
      }

      try:
          response = requests.get(url, params=params, headers=headers)
          flowColumnData = response.json()["result"]["items"]
          print(f"Data successfully imported")

          return flowColumnData

      except requests.exceptions.RequestException as e:

          raise ValueError("An error occurred in GET TOKEN:\n" + str(e))

def UpdateFlowColumn(domaine, oFlowId,  newSourceColumnName, newDestinationColumnName, newDestinationColumnDataType, newSourceColumnDatatype, id,tenantId, token):
      """
      ----------------------------------------------------------------------------------------------------------------------
      GET FLOW COLUMN
      ----------------------------------------------------------------------------------------------------------------------
      """
      url = domaine + "/api/services/app/OProjects/UpdateFlowColumn"

      # Request headers
      headers = {
          "Abp.TenantId": tenantId,
          "Authorization": "Bearer " + token
      }
      # Iterate through items in the updated response JSON
      # Create the payload for the PUT request
      payload = {
          "oFlowId": oFlowId,
          "sourceColumnName": newSourceColumnName,
          "destinationColumnName": newDestinationColumnName,
          "destinationColumnDataType":newDestinationColumnDataType,
          "sourceColumnDatatype": newSourceColumnDatatype,
          "missingInSource": False,
          "id": id
      }
      try:
          response = requests.put(url, json=payload, headers=headers)

          print(f"Source column '{newSourceColumnName}' successfully updated to '{newDestinationColumnName} ({newDestinationColumnDataType})'")

      except requests.exceptions.RequestException as e:

          raise ValueError("An error occurred in GET TOKEN:\n" + str(e))

"""
----------------------------------------------------------------------------------------------------------------------
CREATE PROJECT
----------------------------------------------------------------------------------------------------------------------
"""

def createProject(domaine,projectName, organizationUnitId,tenantId, token, showConnections=True, showNotifications=False,
                  showFlows=True,showForms=False,showSchedules=True, showSQLScripts=True, showShellScripts=False,
                  showFileProviders=False, showVariables=False ,showWorkflows=True, showReports=False, showWidgets=False):
     url = domaine + "/api/services/app/OProjects/CreateOrEdit"

     payload = json.dumps({
        "name": projectName,
        "organizationUnitId": organizationUnitId,
        "showConnections": showConnections,
        "showNotifications": showNotifications,
        "showFlows": showFlows,
        "showForms": showForms,
        "showSchedules": showSchedules,
        "showSQLScripts": showSQLScripts,
        "showShellScripts": showShellScripts,
        "showFileProviders": showFileProviders,
        "showVariables": showVariables,
        "showWorkflows": showWorkflows,
        "showReports": showReports,
        "showWidgets": showWidgets
      })
     headers = {
        'Abp.TenantId': tenantId,
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
      }


     try:
        response = requests.request("POST", url, headers=headers, data=payload)
        projectId=str(response.json()['result'])
        print(f"Porject '{projectName}' successfully created")
        return projectId

     except requests.exceptions.RequestException as e:

        raise ValueError("An error occurred in GET TOKEN:\n" + str(e))


"""
----------------------------------------------------------------------------------------------------------------------
CREATE SQL SCRIPT
----------------------------------------------------------------------------------------------------------------------
"""

def createSqlScript(domaine,scriptName,createSqlScriptQuery,projectId,connectionId,tenantId, token):
    url = domaine + "/api/services/app/OSQLScripts/CreateOrEdit"

    payload = json.dumps({
        "name": scriptName,
        "query": createSqlScriptQuery,
        "commandTimeout": 60,
        "oProjectId": projectId,
        "oConnectionId": connectionId
    })
    headers = {
        'Abp.TenantId': tenantId,
        'Accept': 'text/plain',
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        scriptId = str(response.json()['result'])
        print(f"Script '{scriptName}' successfully created")
        return scriptId

    except requests.exceptions.RequestException as e:

        raise ValueError("An error occurred in GET TOKEN:\n" + str(e))
        
"""
----------------------------------------------------------------------------------------------------------------------
START WORKFLOW
----------------------------------------------------------------------------------------------------------------------
"""        

def StartWorkFlow(domaine,workFlowId,tenantId, token):
    
    url = domaine + "/api/services/app/OWorkflows/Start"

    payload = json.dumps({
      "id": workFlowId
    })
    headers = {
        'Abp.TenantId': tenantId,
        'Accept': 'text/plain',
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
    }

    try:
         response = requests.request("POST", url, headers=headers, data=payload)
         scriptId = str(response.json()['result'])
         print(f"WorkFlow successfully created")
         return scriptId

    except requests.exceptions.RequestException as e:

         raise ValueError("An error occurred in StartWorkFow:\n" + str(e))
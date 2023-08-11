import json
import requests

def getToken(domaine,username,pwd,tenantId):
    """
    ----------------------------------------------------------------------------------------------------------------------
    GET TOKEN
    ----------------------------------------------------------------------------------------------------------------------
    """

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
    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        token = str(response.json()['result']['accessToken'])
        print("Accessed bearer token and connexion successfull")
        return token
    except requests.exceptions.RequestException as e:

        raise ValueError ("An error occurred in GET TOKEN:\n" + str(e))


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
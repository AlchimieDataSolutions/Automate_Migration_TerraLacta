import json
import requests

# Define the StartWorkFlow function
def StartWorkFlow(domain, tenantId, workFlowId, token):
    url = f"{domain}/api/services/app/OWorkflows/Start"

    payload = json.dumps({
        "id": workFlowId
    })
    headers = {
        'Abp.TenantId': tenantId,
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, data=payload)
        response_json = response.json()

        if response.status_code == 200:
            scriptId = response_json.get('result')
            print("Workflow successfully created")
            return scriptId
        else:
            raise Exception(f"StartWorkFlow failed with status code: {response.status_code}, {response_json}")

    except requests.exceptions.RequestException as e:
        raise ValueError("An error occurred in StartWorkFow:\n" + str(e))

# Define the getToken function
def getToken(domain, username, pwd, tenantId):
    url = f"{domain}/api/TokenAuth/Authenticate"

    payload = json.dumps({
        "userNameOrEmailAddress": username,
        "password": pwd
    })
    headers = {
        'Abp.TenantId': tenantId,
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, data=payload)

        if response.status_code == 200:
            token = response.json().get('result').get('accessToken')
            print("Accessed bearer token and connection successful")
            return token
        else:
            raise Exception(f"Connection failed with status code: {response.status_code}, {response.json()}")

    except requests.exceptions.RequestException as e:
        raise ValueError("An error occurred in getToken:\n" + str(e))

# API PARAMETERS
api_user = "kasi.gajavalli@alchimiedatasolutions.com"
api_password = "6nEbNvmmTbLZR9U"
api_domain = "https://onyx-back.azurewebsites.net"
api_tenantId = "14"

# WorkFlowID
workflowId = "07d55659-ec77-4ffb-9893-49d46a866aa1"

# GetToken
token = getToken(api_domain, api_user, api_password, api_tenantId)

# StartWorkFlow
executeWorkFlow = StartWorkFlow(api_domain, api_tenantId, workflowId, token)

#MSSQL CONNECTION PARAMAETERS
dwh_server='10.101.5.85'
dwh_user='app_onyx_dwh_prod'
dwh_password='poca59100!!'
dwh_database='onyx_core_prod'

#API PARAMETERS
api_user="antoine.ducoulombier@alchimiedatasolutions.com"
api_password="Casagamd58"
api_domain="https://onyx-back.azurewebsites.net"
api_tenantId="14"

# FLUX NEXT
nomFluxNext = "AD.QN.800 - WILEO SLVA"
nomObjet = "800_WILEO_SLVA"
projectId = "6d364624-f0b1-4731-8605-65e5e082e81c"
destinationConnectionId = "46731818-1b4d-4a8b-a00d-5136a7d18c8f"

getProject="""
SELECT TOP 3 t.tbl_lib, CONCAT('STA_',t.tbl_lib_new) as 'nom',
CASE   WHEN c.cnn_lib = '101L5_DIVSQL2 - MSSQLWILEO' THEN '6f40c6b4-1c59-4601-9211-71b70f0e0513'
WHEN c.cnn_lib = 'DIVALTO DIFVETO' THEN '80126575-8fea-49c2-af30-2560b83a2b26'
WHEN c.cnn_lib = 'DIVALTO FAYES' THEN '36800744-3f2f-4cbf-a139-4da397a5a8d7'
WHEN c.cnn_lib = 'DIVALTO LMDL' THEN 'cbed7ed3-df8d-4696-82dd-152990f9cc39'
WHEN c.cnn_lib = 'DIVALTO ORGEVAL' THEN '42ec48f0-ca9b-46cf-be0a-c64c967bf139'
WHEN c.cnn_lib = 'DIVALTO SLVA' THEN 'f0761fec-6aa3-4d31-9c4d-5abb93caa117'
WHEN c.cnn_lib = 'DIVALTO SLVA (ONYX_Temp)' THEN 'a070e3a0-3c61-4977-bb74-090b3c5bdcc5'
WHEN c.cnn_lib = 'DIVALTO SQL2_TEMP' THEN 'ff5db02f-de25-47a9-8e11-85b879e10ab1'
WHEN c.cnn_lib = 'DIVALTO SQL3_TEMP' THEN '5631daf1-6223-4e04-933d-ddd38d2057b1'
WHEN c.cnn_lib = 'DIVALTO TERRALACTA' THEN '5af589e1-4dde-4e8f-86e5-bbd062087886'
WHEN c.cnn_lib = 'ISIMEDIA' THEN '5b107c96-3a84-486b-93d1-3f44dafa2a6f'
WHEN c.cnn_lib = 'MILK_OFFICE' THEN 'b2f086f2-1155-4a8f-83b3-288bbfabf953'
WHEN c.cnn_lib = 'MILK_OFFICE LICENCES' THEN 'ef7d5391-a4f8-4514-ad16-562c946dfd0a'
WHEN c.cnn_lib = 'MILK_OFFICE MAT' THEN '9b1abff3-882e-4de3-877f-0caf2bf1adcb'
WHEN c.cnn_lib = 'MILK_OFFICE (TL_TEMP)' THEN '96685fee-ad82-4ea7-9335-97a25de8fc57'
WHEN c.cnn_lib = 'Referentiel produit' THEN '91b38d9a-3494-4b7b-bb3b-737fa8edd6c2'
WHEN c.cnn_lib = 'Reseau GLAC' THEN 'c67042b4-7464-4e67-b53b-9c7dd8644d6b'
WHEN c.cnn_lib = 'SOLID' THEN '5d313550-2f92-4251-ae4d-171a488a2e00'
WHEN c.cnn_lib = 'STAMBIA' THEN '82acb0a0-35a3-4dea-8f7d-8d90bc084a9c'
WHEN c.cnn_lib = 'TERRA_LACTA_DWH' THEN '46731818-1b4d-4a8b-a00d-5136a7d18c8f'
WHEN c.cnn_lib = 'Weavy - Divalto - Customers' THEN '24ad2644-fcad-4d4f-b0cd-6508fffdd9cc'
WHEN c.cnn_lib = 'Weavy - Divalto - Equipment Address' THEN '828e788e-7457-4d75-93c0-1aab8fb93602'
WHEN c.cnn_lib = 'Weavy - Divalto - Equipments' THEN '9864d7fa-db14-49f6-9485-deb6807c17f2'
WHEN c.cnn_lib = 'Weavy - Divalto - Intervention Parts Details' THEN 'bca14a35-f3b2-436f-a703-d51b27f773c0'
WHEN c.cnn_lib = 'Weavy - Divalto - Interventions' THEN 'bc544eda-10ab-4bb3-a806-53f3435cff7c'
WHEN c.cnn_lib = 'Weavy - Divalto - Interventions Equipments' THEN 'ed6421c0-f463-4e95-b9b2-1d4fcf636c83'
WHEN c.cnn_lib = 'Weavy - Divalto - Interventions History Headers' THEN 'a3cad626-937a-4a4c-910c-53c1c7b6fac2'
WHEN c.cnn_lib = 'Weavy - Divalto - Interventions Parts' THEN 'f8086ea7-36e2-420e-ae68-957ef9813de1'
WHEN c.cnn_lib = 'Weavy - Divalto - Intervention Types' THEN '5d445d2b-0db3-48e5-9d04-68cb7eb8525c'
WHEN c.cnn_lib = 'Weavy - Divalto - Planning' THEN 'f9e12fce-6c08-492c-a409-016632b91921'
WHEN c.cnn_lib = 'Weavy - Divalto - Users' THEN 'edb5282c-6c80-4712-85ad-cc31975145fb'
WHEN c.cnn_lib = 'WILEO SLVA' THEN '73f35d5d-ba39-4f1f-abcc-6772810625b5'
END AS cnn_lib_mapping,
c.cnn_lib

FROM
[sn_orchestrator].[t_project_prj] p
JOIN [sn_collect].[t_table_tbl] t ON p.prj_id = t.prj_id
JOIN [sn_collect].[t_connection_cnn] c on c.cnn_id=t.cnn_id
WHERE
p.[prj_lib] = '{}';
"""

getFlowColumns="""
	 SELECT top 5
     c.clm_lib

    FROM
    [sn_orchestrator].[t_project_prj] p
    JOIN [sn_collect].[t_table_tbl] t ON p.prj_id = t.prj_id
    JOIN [sn_collect].[t_column_clm] c ON t.tbl_id = c.tbl_id
    JOIN [sn_collect].[t_connection_cnn] cn on cn.cnn_id=t.cnn_id
    WHERE
        p.[prj_lib] = '{}'
        AND
        t.tbl_lib = '{}'
        AND
        cn.cnn_lib='{}'
"""
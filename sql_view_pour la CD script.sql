CREATE VIEW [dbo].[STA_DVT_WEAVY_interventionequipment_view] AS
SELECT
    sn_file_name,
    sn_file_rownum,
    JSON_VALUE(t.line, '$.srvExport') AS srvExport,
    JSON_VALUE(t.line, '$.srvDateUTC') AS srvDateUTC,
    JSON_VALUE(t.line, '$.srvAttrib') AS srvAttrib,
    JSON_VALUE(t.line, '$.interventionequipment_ID') AS interventionequipment_ID,
    JSON_VALUE(t.line, '$.intervention_ID') AS intervention_ID,
    JSON_VALUE(t.line, '$.equipment_ID') AS equipment_ID,
    JSON_VALUE(t.line, '$.addedByUser') AS addedByUser,
    JSON_VALUE(t.line, '$.contract_ID') AS contract_ID
FROM [dbo].[STA_DVT_WEAVY_interventionequipment_json] t;

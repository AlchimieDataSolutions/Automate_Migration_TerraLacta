import pymssql

class CDCScriptGenerator_MSSQL_Type1:
    def generate_insert_query(self, destination, cols, cols_source, colHash, colCreationDate, source, s, destination_alias, join_clause, first_key):
        insert_query = f"""
-- Data insert

INSERT INTO {destination} ({cols}, {colHash}, {colCreationDate})
SELECT {cols_source}, HASHBYTES('MD5', (SELECT {cols_source} FOR XML RAW)), GETDATE()
FROM
    {source} AS {s}
    LEFT JOIN {destination} AS {destination_alias} ON {join_clause}
WHERE
    {destination_alias}.{first_key} IS NULL;
"""
        return insert_query

    def generate_update_query(self, destination, cols_update, cols_source, colHash, colUpdateDate, source, s, destination_alias, join_clause):
        update_query = f"""
-- Data update

UPDATE {destination} AS {destination_alias}
SET
    {cols_update},
    {colHash} = HASHBYTES('MD5', (SELECT {cols_source} FOR XML RAW)),
    {colUpdateDate} = GETDATE()
FROM
    {source} AS {s}
WHERE
    {join_clause} AND HASHBYTES('MD5', (SELECT {cols_source} FOR XML RAW)) <> {destination_alias}.{colHash};
"""
        return update_query

    def generate_delete_query(self, destination, keys, source, first_key):
        delete_query = f"""
-- Data delete

DELETE FROM {destination}
WHERE
    ({first_key}) NOT IN (SELECT {keys} FROM {source});
"""
        return delete_query

# usage
if __name__ == "__main__":
    destination = "[dbo].[nx_BUS_DVT_WEAVY_interventiontype]"
    cols = "sn_file_name, sn_file_rownum, srvExport, srvDateUTC, srvAttrib, interventiontype_ID, label, translationKey, printTimes, printProducts, defaultDuration, isBreakFix, color, final_y_visite_prv, final_it_emaildest, final_it_enlevement"  # Corrected column list
    cols_source = "sn_file_name, sn_file_rownum, srvExport, srvDateUTC, srvAttrib, interventiontype_ID, label, translationKey, printTimes, printProducts, defaultDuration, isBreakFix, color, final_y_visite_prv, final_it_emaildest, final_it_enlevement"  # Corrected source column list
    colHash = "hash_column"
    colCreationDate = "creation_date_column"
    source = "[dbo].[STA_DVT_WEAVY_interventiontype_json]"  # Corrected source table name
    s = "s"
    destination_alias = "d"
    join_clause = "s.interventiontype_ID = d.interventiontype_ID"  # Corrected join clause based on interventiontype_ID
    first_key = "interventiontype_ID"  # Corrected first key to interventiontype_ID
    cols_update = "sn_file_name = s.sn_file_name, sn_file_rownum = s.sn_file_rownum, srvExport = s.srvExport, srvDateUTC = s.srvDateUTC, srvAttrib = s.srvAttrib, interventiontype_ID = s.interventiontype_ID, label = s.label, translationKey = s.translationKey, printTimes = s.printTimes, printProducts = s.printProducts, defaultDuration = s.defaultDuration, isBreakFix = s.isBreakFix, color = s.color, final_y_visite_prv = s.final_y_visite_prv, final_it_emaildest = s.final_it_emaildest, final_it_enlevement = s.final_it_enlevement"  # Corrected update column list
    keys = "sn_file_name, sn_file_rownum"
    colUpdateDate = "update_date_column"

    script_generator = CDCScriptGenerator_MSSQL_Type1()

    insert_query = script_generator.generate_insert_query(destination, cols, cols_source, colHash, colCreationDate, source, s, destination_alias, join_clause, first_key)  # Pass 'destination' as 'd'
    update_query = script_generator.generate_update_query(destination, cols_update, cols_source, colHash, colUpdateDate, source, s, destination_alias, join_clause)
    delete_query = script_generator.generate_delete_query(destination, keys, source, first_key)

    print(insert_query)
    print(update_query)
    print(delete_query)

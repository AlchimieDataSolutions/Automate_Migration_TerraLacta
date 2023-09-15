
import pymssql

class CDCScriptGenerator_MSSQL_Type1:
    # def __init__(self, server, database, username, password):
        # self.conn = pymssql.connect(server, username, password, database)
        # self.cursor = self.conn.cursor()

    def generate_insert_query(self, destination, cols, cols_source, colHash, colCreationDate, source, s, destination_alias, d, join_clause, first_key):
        insert_query = f"""
-- Data insert

INSERT INTO {destination} ({cols}, {colHash}, {colCreationDate})
SELECT {cols_source}, HASHBYTES('MD5', (SELECT {cols_source} FOR XML RAW)), GETDATE()
FROM
    {source} AS {s}
    LEFT JOIN {destination} AS {d} ON {join_clause}
WHERE
    {d}.{first_key} IS NULL;
"""
        return insert_query

    def generate_update_query(self, destination, cols_update, cols_source, colHash, colUpdateDate, source, s, destination_alias, join_clause):
        update_query = f"""
-- Data update

UPDATE {destination}
SET
    {cols_update},
    {colHash} = HASHBYTES('MD5', (SELECT {cols_source} FOR XML RAW)),
    {colUpdateDate} = GETDATE()
FROM
    {source} AS {s}
WHERE
    {join_clause.replace("[d]", destination_alias)} AND HASHBYTES('MD5', (SELECT {cols_source} FOR XML RAW)) <> {destination_alias}.{colHash};
"""
        return update_query

    def generate_delete_query(self, destination, keys, source):
        delete_query = f"""
-- Data delete

DELETE FROM {destination}
WHERE
    ({first_key}) NOT IN (SELECT {keys} FROM {source});
"""
        return delete_query

    def close_connection(self):
        # self.conn.close()

# usage
if __name__ == "__main__":
    # MSSQL CONNECTION PARAMETERS 
    server = '10.101.5.85'
    username = 'app_onyx_dwh_prod'
    password = 'poca59100!!'
    database = 'onyx_core_prod'

    script_generator = CDCScriptGenerator_MSSQL_Type1()
        # server, database, username, password)

    destination = "[dbo].[nx_BUS_TEST_DVT_WEAVY_interventionequipment]"
    cols = "sn_file_name, sn_file_rownum, srvExport, srvDateUTC, srvAttrib, interventionequipment_ID, intervention_ID, equipment_ID, addedByUser, contract_ID"
    cols_source = "sn_file_name, sn_file_rownum, srvExport, srvDateUTC, srvAttrib, interventionequipment_ID, intervention_ID, equipment_ID, addedByUser, contract_ID"

    colHash = "hash_column"
    colCreationDate = "creation_date_column"
    source = "[dbo].[STA_DVT_WEAVY_interventionequipment_view]"
    s = "s"
    destination_alias = "d"
    join_clause = "s.interventionequipment_ID = d.interventionequipment_ID"
    first_key = "interventionequipment_ID"
    cols_update = "sn_file_name = s.sn_file_name, sn_file_rownum = s.sn_file_rownum, srvExport = s.srvExport, srvDateUTC = s.srvDateUTC, srvAttrib = s.srvAttrib, intervention_ID = s.intervention_ID, equipment_ID = s.equipment_ID, addedByUser = s.addedByUser, contract_ID = s.contract_ID"
    keys = "sn_file_name, sn_file_rownum"
    colUpdateDate = "update_date_column" 

    insert_query = script_generator.generate_insert_query(destination, cols, cols_source, colHash, colCreationDate, source, s, destination_alias, destination, join_clause, first_key)  # Pass 'destination' as 'd'
    update_query = script_generator.generate_update_query(destination, cols_update, cols_source, colHash, colUpdateDate, source, s, destination_alias, join_clause)
    delete_query = script_generator.generate_delete_query(destination, keys, source)

    print(insert_query)
    print(update_query)
    print(delete_query)

    script_generator.close_connection()

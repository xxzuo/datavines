/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.datavines.connector.plugin;

import io.datavines.common.datasource.jdbc.*;
import io.datavines.common.datasource.jdbc.entity.TableInfo;
import io.datavines.common.datasource.jdbc.utils.JdbcDataSourceUtils;
import io.datavines.common.param.ConnectorResponse;
import io.datavines.common.param.GetTablesRequestParam;
import io.datavines.common.param.form.type.InputParam;
import io.datavines.common.utils.JSONUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class SqlserverConnector extends JdbcConnector {

    private final JdbcExecutorClientManager jdbcExecutorClientManager = JdbcExecutorClientManager.getInstance();

    @Override
    public BaseJdbcDataSourceInfo getDatasourceInfo(JdbcConnectionInfo jdbcConnectionInfo) {
        return new SqlserverDataSourceInfo(jdbcConnectionInfo);
    }

    @Override
    public ResultSet getMetadataColumns(DatabaseMetaData metaData, String catalog, String schema, String tableName, String columnName) throws SQLException {
//        return metaData.getColumns(schema, null, tableName, columnName);
        return metaData.getColumns(schema, null, tableName.split("\\.")[1], columnName);
    }

    @Override
    public ResultSet getMetadataTables(DatabaseMetaData metaData, String catalog, String schema) throws SQLException {
//        return metaData.getTables(schema, null, null, TABLE_TYPES);
        return metaData.getTables(catalog, schema, null, TABLE_TYPES);
    }

    @Override
    public ResultSet getMetadataDatabases(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        return metaData.getCatalogs();
    }

    @Override
    public ResultSet getMetadataSchemas(DatabaseMetaData metaData, String catalog) throws SQLException {
        return metaData.getSchemas(catalog, null);
    }

    @Override
    protected List<String> getSchemaList(DatabaseMetaData metaData, String catalog) throws SQLException {
        List<String> schemaList = new ArrayList<>();
        ResultSet metadataSchemas = getMetadataSchemas(metaData, catalog);
        List<String> filterTableList = Arrays.asList("db_owner", "db_accessadmin", "db_backupoperator", "db_datareader", "db_datawriter", "db_ddladmin", "db_denydatareader", "db_denydatawriter", "db_securityadmin", "guest", "information_schema", "sys");
        HashSet<String> filterSet = new HashSet<>(filterTableList);
        if(metadataSchemas != null){
            while (metadataSchemas.next()){
                if(!filterSet.contains(metadataSchemas.getString("TABLE_SCHEM").toLowerCase())){
                    schemaList.add(metadataSchemas.getString("TABLE_SCHEM"));
                }
            }
        }
        return schemaList;
    }

    @Override
    protected ResultSet getPrimaryKeys(DatabaseMetaData metaData,String catalog, String schema, String tableName) throws SQLException {
        return metaData.getPrimaryKeys(schema, null, tableName.split("\\.")[1]);
    }

}

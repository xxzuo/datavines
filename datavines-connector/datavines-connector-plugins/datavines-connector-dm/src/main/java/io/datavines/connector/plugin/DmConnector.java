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

import io.datavines.common.datasource.jdbc.BaseJdbcDataSourceInfo;
import io.datavines.common.datasource.jdbc.JdbcConnectionInfo;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DmConnector  extends JdbcConnector {
    @Override
    public BaseJdbcDataSourceInfo getDatasourceInfo(JdbcConnectionInfo jdbcConnectionInfo) {
        return new DmDataSourceInfo(jdbcConnectionInfo);
    }

    @Override
    protected ResultSet getMetadataDatabases(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        // DM only need schema
        return metaData.getSchemas();
    }

    protected ResultSet getMetadataTables(DatabaseMetaData metaData, String catalog, String schema) throws SQLException {
        // DM only need schema
        return metaData.getTables(null, schema, null, TABLE_TYPES);
    }

    protected ResultSet getMetadataColumns(DatabaseMetaData metaData,
                                           String catalog, String schema,
                                           String tableName, String columnName) throws SQLException {
        // DM only need schema
        return metaData.getColumns(null, schema, tableName, columnName);
    }

    protected ResultSet getPrimaryKeys(DatabaseMetaData metaData,String catalog, String schema, String tableName) throws SQLException {
        // DM only need schema
        return metaData.getPrimaryKeys(null, schema, tableName);
    }
}

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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.datavines.common.ConfigConstants.DATABASE;
import static io.datavines.common.ConfigConstants.HOST;
import static io.datavines.common.ConfigConstants.PORT;
import static io.datavines.common.ConfigConstants.PROPERTIES;
import static io.datavines.common.ConfigConstants.SCHEMA;

public class PostgreSqlDataSourceInfoTest {

    private Map<String, String> buildParam(String schema, String properties) {
        Map<String, String> param = new HashMap<>();
        param.put(HOST, "127.0.0.1");
        param.put(PORT, "5432");
        param.put(DATABASE, "rms_hu");
        param.put(SCHEMA, schema);
        param.put(PROPERTIES, properties);
        return param;
    }

    @Test
    public void testJdbcUrlWithConfiguredSchema() {
        PostgreSqlDataSourceInfo dataSourceInfo = new PostgreSqlDataSourceInfo(buildParam("inter", null));

        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=inter", dataSourceInfo.getJdbcUrl());
    }

    @Test
    public void testJdbcUrlWithoutConfiguredSchema() {
        PostgreSqlDataSourceInfo dataSourceInfo = new PostgreSqlDataSourceInfo(buildParam(null, null));

        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu", dataSourceInfo.getJdbcUrl());
    }

    @Test
    public void testExplicitCurrentSchemaInPropertiesTakesPrecedence() {
        PostgreSqlDataSourceInfo dataSourceInfo = new PostgreSqlDataSourceInfo(buildParam("inter", "currentSchema=other"));

        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=other", dataSourceInfo.getJdbcUrl());
    }

    @Test
    public void testPoolKeyContainsSchema() {
        PostgreSqlDataSourceInfo inter = new PostgreSqlDataSourceInfo(buildParam("inter", null));
        PostgreSqlDataSourceInfo publicSchema = new PostgreSqlDataSourceInfo(buildParam("public", null));

        // datasources differing only in schema must not share a connection pool
        Assert.assertNotEquals(inter.getUniqueKey(), publicSchema.getUniqueKey());
    }
}

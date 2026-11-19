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
import static io.datavines.common.ConfigConstants.URL;

public class PostgreSqlParameterConverterTest {

    private final PostgreSqlParameterConverter converter = new PostgreSqlParameterConverter();

    private Map<String, Object> buildParameter(String schema, String properties) {
        Map<String, Object> parameter = new HashMap<>();
        parameter.put(HOST, "127.0.0.1");
        parameter.put(PORT, "5432");
        parameter.put(DATABASE, "rms_hu");
        parameter.put(SCHEMA, schema);
        parameter.put(PROPERTIES, properties);
        return parameter;
    }

    @Test
    public void testUrlWithConfiguredSchema() {
        Map<String, Object> config = converter.converter(buildParameter("inter", null));

        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=inter", config.get(URL));
    }

    @Test
    public void testUrlWithoutConfiguredSchema() {
        Map<String, Object> config = converter.converter(buildParameter(null, null));

        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu", config.get(URL));
    }

    @Test
    public void testUrlWithEmptyConfiguredSchema() {
        Map<String, Object> config = converter.converter(buildParameter("", null));

        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu", config.get(URL));
    }

    @Test
    public void testUrlWithPropertiesAndConfiguredSchema() {
        Map<String, Object> config = converter.converter(buildParameter("inter", "connectTimeout=10"));

        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?connectTimeout=10&currentSchema=inter", config.get(URL));
    }

    @Test
    public void testExplicitCurrentSchemaInPropertiesTakesPrecedence() {
        Map<String, Object> config = converter.converter(buildParameter("inter", "currentSchema=other&connectTimeout=10"));

        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=other&connectTimeout=10", config.get(URL));
    }

    @Test
    public void testExistingUrlGetsCurrentSchemaAppended() {
        Map<String, Object> parameter = buildParameter("inter", null);
        parameter.put(URL, "jdbc:postgresql://127.0.0.1:5432/rms_hu");

        Map<String, Object> config = converter.converter(parameter);

        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=inter", config.get(URL));
    }

    @Test
    public void testExistingUrlWithCurrentSchemaIsKept() {
        Map<String, Object> parameter = buildParameter("inter", null);
        parameter.put(URL, "jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=other");

        Map<String, Object> config = converter.converter(parameter);

        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=other", config.get(URL));
    }
}

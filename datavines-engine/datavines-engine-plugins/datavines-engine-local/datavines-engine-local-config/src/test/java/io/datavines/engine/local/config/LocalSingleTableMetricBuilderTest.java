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
package io.datavines.engine.local.config;

import io.datavines.common.config.SourceConfig;
import io.datavines.common.entity.ConnectorParameter;
import io.datavines.common.entity.JobExecutionInfo;
import io.datavines.common.entity.JobExecutionParameter;
import io.datavines.common.entity.job.BaseJobParameter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datavines.common.CommonConstants.DATABASE2;
import static io.datavines.common.CommonConstants.TABLE2;
import static io.datavines.common.ConfigConstants.ACTUAL_AGGREGATE_SQL;
import static io.datavines.common.ConfigConstants.DATABASE;
import static io.datavines.common.ConfigConstants.HOST;
import static io.datavines.common.ConfigConstants.PASSWORD;
import static io.datavines.common.ConfigConstants.PORT;
import static io.datavines.common.ConfigConstants.SCHEMA;
import static io.datavines.common.ConfigConstants.TABLE;
import static io.datavines.common.ConfigConstants.URL;
import static io.datavines.common.ConfigConstants.USER;

/**
 * Configuration building tests for custom SQL metrics on PostgreSQL,
 * covering the separation between the parsed table's schema and the
 * connection's default schema (search_path / currentSchema).
 */
public class LocalSingleTableMetricBuilderTest {

    private ConnectorParameter buildPgConnector(String schema) {
        ConnectorParameter connectorParameter = new ConnectorParameter();
        connectorParameter.setType("postgresql");
        Map<String, Object> parameters = new HashMap<>();
        parameters.put(HOST, "127.0.0.1");
        parameters.put(PORT, "5432");
        parameters.put(DATABASE, "rms_hu");
        parameters.put(SCHEMA, schema);
        parameters.put(USER, "postgres");
        parameters.put(PASSWORD, "postgres");
        connectorParameter.setParameters(parameters);
        return connectorParameter;
    }

    private LocalSingleTableMetricBuilder buildBuilder(String schema, String sql, ConnectorParameter target) {
        Map<String, Object> metricParameter = new HashMap<>();
        metricParameter.put(ACTUAL_AGGREGATE_SQL, sql);
        metricParameter.put(DATABASE, "rms_hu");
        metricParameter.put(TABLE, "mid_flt_price_ivs");
        if (target != null) {
            metricParameter.put(DATABASE2, "rms_hu");
            metricParameter.put(TABLE2, "target_table");
        }

        BaseJobParameter jobParameter = new BaseJobParameter();
        jobParameter.setMetricType("custom_aggregate_sql");
        jobParameter.setMetricParameter(metricParameter);
        jobParameter.setExpectedType("fix_value");

        JobExecutionParameter jobExecutionParameter = new JobExecutionParameter();
        jobExecutionParameter.setMetricParameterList(Collections.singletonList(jobParameter));
        jobExecutionParameter.setConnectorParameter(buildPgConnector(schema));
        jobExecutionParameter.setConnectorParameter2(target);

        JobExecutionInfo jobExecutionInfo = new JobExecutionInfo();
        jobExecutionInfo.setId(1L);
        jobExecutionInfo.setName("test");
        jobExecutionInfo.setEngineType("local");
        jobExecutionInfo.setErrorDataStorageType("file");
        jobExecutionInfo.setErrorDataStorageParameter("{\"data_dir\":\"/tmp/datavines\"}");
        jobExecutionInfo.setErrorDataFileName("error_data");
        jobExecutionInfo.setValidateResultDataStorageType("file");
        jobExecutionInfo.setValidateResultDataStorageParameter("{\"data_dir\":\"/tmp/datavines\"}");
        jobExecutionInfo.setJobExecutionParameter(jobExecutionParameter);

        LocalSingleTableMetricBuilder builder = new LocalSingleTableMetricBuilder();
        builder.init(new HashMap<>(), jobExecutionInfo);
        return builder;
    }

    @Test
    public void testConnectionSchemaStaysConfiguredWhenSqlUsesOtherSchema() throws Exception {
        // the unqualified "users" must still resolve against the configured schema
        // public, not against the first parsed table's schema other_schema
        LocalSingleTableMetricBuilder builder = buildBuilder("public",
                "select count(1) as actual_value from other_schema.events join users on users.id = events.id", null);

        List<SourceConfig> sourceConfigs = builder.getSourceConfigs();

        Assert.assertEquals(1, sourceConfigs.size());
        Map<String, Object> config = sourceConfigs.get(0).getConfig();
        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=public", config.get(URL));
        Assert.assertEquals("public", config.get(SCHEMA));
        // the parsed table itself keeps its own schema
        Assert.assertEquals("rms_hu.other_schema.events", config.get(TABLE));
    }

    @Test
    public void testUnqualifiedTableResolvesAgainstConfiguredSchema() throws Exception {
        LocalSingleTableMetricBuilder builder = buildBuilder("inter",
                "select max(flt_date) as actual_value from mid_flt_price_ivs", null);

        List<SourceConfig> sourceConfigs = builder.getSourceConfigs();

        Map<String, Object> config = sourceConfigs.get(0).getConfig();
        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=inter", config.get(URL));
        Assert.assertEquals("rms_hu.inter.mid_flt_price_ivs", config.get(TABLE));
    }

    @Test
    public void testNoCurrentSchemaWhenSchemaNotConfigured() throws Exception {
        LocalSingleTableMetricBuilder builder = buildBuilder(null,
                "select count(1) as actual_value from public.events", null);

        List<SourceConfig> sourceConfigs = builder.getSourceConfigs();

        Map<String, Object> config = sourceConfigs.get(0).getConfig();
        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu", config.get(URL));
        Assert.assertNull(config.get(SCHEMA));
        Assert.assertEquals("rms_hu.public.events", config.get(TABLE));
    }

    @Test
    public void testTargetConnectionUsesItsOwnConfiguredSchema() throws Exception {
        // the source's schema must not leak into the target connection
        LocalSingleTableMetricBuilder builder = buildBuilder("inter",
                "select count(1) as actual_value from inter.events", buildPgConnector("public"));

        List<SourceConfig> sourceConfigs = builder.getSourceConfigs();

        Assert.assertEquals(2, sourceConfigs.size());
        Map<String, Object> sourceConfig = sourceConfigs.get(0).getConfig();
        Map<String, Object> targetConfig = sourceConfigs.get(1).getConfig();
        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=inter", sourceConfig.get(URL));
        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=public", targetConfig.get(URL));
        Assert.assertEquals("rms_hu.public.target_table", targetConfig.get(TABLE));
    }
}

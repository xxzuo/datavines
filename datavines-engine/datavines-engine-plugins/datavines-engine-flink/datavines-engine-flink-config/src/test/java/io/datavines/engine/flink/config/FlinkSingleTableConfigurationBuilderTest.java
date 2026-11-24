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
package io.datavines.engine.flink.config;

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
 * The Flink JDBC source executes "select * from TABLE" and uses TABLE as the
 * JDBC connector 'table-name', so TABLE must carry the table's own qualified
 * name while SCHEMA / currentSchema stay the datasource-configured connection default.
 */
public class FlinkSingleTableConfigurationBuilderTest {

    private FlinkSingleTableConfigurationBuilder buildBuilder(String schema, String sql) {
        Map<String, Object> metricParameter = new HashMap<>();
        metricParameter.put(ACTUAL_AGGREGATE_SQL, sql);
        metricParameter.put(DATABASE, "rms_hu");
        metricParameter.put(TABLE, "mid_flt_price_ivs");

        BaseJobParameter jobParameter = new BaseJobParameter();
        jobParameter.setMetricType("custom_aggregate_sql");
        jobParameter.setMetricParameter(metricParameter);
        jobParameter.setExpectedType("fix_value");

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

        JobExecutionParameter jobExecutionParameter = new JobExecutionParameter();
        jobExecutionParameter.setMetricParameterList(Collections.singletonList(jobParameter));
        jobExecutionParameter.setConnectorParameter(connectorParameter);

        JobExecutionInfo jobExecutionInfo = new JobExecutionInfo();
        jobExecutionInfo.setId(1L);
        jobExecutionInfo.setName("test");
        jobExecutionInfo.setEngineType("flink");
        jobExecutionInfo.setErrorDataStorageType("file");
        jobExecutionInfo.setErrorDataStorageParameter("{\"data_dir\":\"/tmp/datavines\"}");
        jobExecutionInfo.setErrorDataFileName("error_data");
        jobExecutionInfo.setValidateResultDataStorageType("file");
        jobExecutionInfo.setValidateResultDataStorageParameter("{\"data_dir\":\"/tmp/datavines\"}");
        jobExecutionInfo.setJobExecutionParameter(jobExecutionParameter);

        FlinkSingleTableConfigurationBuilder builder = new FlinkSingleTableConfigurationBuilder();
        builder.init(new HashMap<>(), jobExecutionInfo);
        return builder;
    }

    @Test
    public void testSourceReadsTableWithItsOwnSchema() throws Exception {
        // "select * from TABLE" must read other_schema.events, not public.events,
        // while the connection default schema stays public for unqualified tables
        FlinkSingleTableConfigurationBuilder builder = buildBuilder("public",
                "select count(1) as actual_value from other_schema.events");

        List<SourceConfig> sourceConfigs = builder.getSourceConfigs();

        Assert.assertEquals(1, sourceConfigs.size());
        Map<String, Object> config = sourceConfigs.get(0).getConfig();
        Assert.assertEquals("other_schema.events", config.get(TABLE));
        Assert.assertEquals("public", config.get(SCHEMA));
        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=public", config.get(URL));
    }

    @Test
    public void testUnqualifiedTableUsesConfiguredSchema() throws Exception {
        FlinkSingleTableConfigurationBuilder builder = buildBuilder("inter",
                "select max(flt_date) as actual_value from mid_flt_price_ivs");

        List<SourceConfig> sourceConfigs = builder.getSourceConfigs();

        Map<String, Object> config = sourceConfigs.get(0).getConfig();
        Assert.assertEquals("inter.mid_flt_price_ivs", config.get(TABLE));
        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu?currentSchema=inter", config.get(URL));
    }

    @Test
    public void testMultiTableWithoutConfiguredSchemaDoesNotLeakSchema() throws Exception {
        // the unqualified second table must not inherit the first table's schema
        FlinkSingleTableConfigurationBuilder builder = buildBuilder(null,
                "select count(1) as actual_value from other_schema.a join b on b.id = a.id");

        List<SourceConfig> sourceConfigs = builder.getSourceConfigs();

        Assert.assertEquals(2, sourceConfigs.size());
        Map<String, Object> first = sourceConfigs.get(0).getConfig();
        Map<String, Object> second = sourceConfigs.get(1).getConfig();
        Assert.assertEquals("other_schema.a", first.get(TABLE));
        Assert.assertNull(first.get(SCHEMA));
        Assert.assertEquals("b", second.get(TABLE));
        Assert.assertNull(second.get(SCHEMA));
        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu", second.get(URL));
    }

    @Test
    public void testUnqualifiedTableWithoutConfiguredSchemaKeepsBareName() throws Exception {
        FlinkSingleTableConfigurationBuilder builder = buildBuilder(null,
                "select max(flt_date) as actual_value from mid_flt_price_ivs");

        List<SourceConfig> sourceConfigs = builder.getSourceConfigs();

        Map<String, Object> config = sourceConfigs.get(0).getConfig();
        // database.table would be misread as schema.table by PostgreSQL
        Assert.assertEquals("mid_flt_price_ivs", config.get(TABLE));
        Assert.assertNull(config.get(SCHEMA));
        Assert.assertEquals("jdbc:postgresql://127.0.0.1:5432/rms_hu", config.get(URL));
    }
}

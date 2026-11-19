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
package io.datavines.engine.config;

import org.junit.Assert;
import org.junit.Test;

public class MetricParserUtilsTest {

    @Test
    public void testParseSingleTableName() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("mid_flt_price_ivs", "inter", false);

        Assert.assertNull(table.getDatabase());
        Assert.assertNull(table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParseSchemaQualifiedTableName() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("inter.mid_flt_price_ivs", "inter", false);

        // the schema prefix must not be treated as the database
        Assert.assertNull(table.getDatabase());
        Assert.assertEquals("inter", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParseSchemaQualifiedTableNameIgnoreCase() {
        // PostgreSQL folds unquoted identifiers to lower case, so INTER.table
        // refers to the same schema as the configured "inter"
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("INTER.mid_flt_price_ivs", "inter", false);

        Assert.assertNull(table.getDatabase());
        Assert.assertEquals("inter", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParseSchemaQualifiedTableNameWithMixedCaseConfiguredSchema() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("inter.mid_flt_price_ivs", "Inter", false);

        Assert.assertNull(table.getDatabase());
        Assert.assertEquals("Inter", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParseDatabaseQualifiedTableName() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("other_db.mid_flt_price_ivs", "inter", false);

        Assert.assertEquals("other_db", table.getDatabase());
        Assert.assertNull(table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParseDatabaseQualifiedTableNameWithoutConfiguredSchema() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("other_db.mid_flt_price_ivs", null, false);

        Assert.assertEquals("other_db", table.getDatabase());
        Assert.assertNull(table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParseFullyQualifiedTableName() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("rms_hu.inter.mid_flt_price_ivs", "inter", false);

        // the last segment is the real table name, not the schema segment
        Assert.assertEquals("rms_hu", table.getDatabase());
        Assert.assertEquals("inter", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParseFullyQualifiedTableNameWithDifferentSchema() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("rms_hu.other_schema.mid_flt_price_ivs", "inter", false);

        Assert.assertEquals("rms_hu", table.getDatabase());
        Assert.assertEquals("other_schema", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParseFullyQualifiedTableNameIgnoreCase() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("RMS_HU.INTER.mid_flt_price_ivs", "inter", false);

        Assert.assertEquals("RMS_HU", table.getDatabase());
        Assert.assertEquals("INTER", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParsePgTwoPartNameIsAlwaysSchemaQualified() {
        // PostgreSQL does not support cross-database references: public.table
        // is schema.table even when the configured schema is different
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("public.mid_flt_price_ivs", "inter", true);

        Assert.assertNull(table.getDatabase());
        Assert.assertEquals("public", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParsePgTwoPartNameWithoutConfiguredSchema() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("public.mid_flt_price_ivs", null, true);

        Assert.assertNull(table.getDatabase());
        Assert.assertEquals("public", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParsePgTwoPartNameMatchesConfiguredSchema() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("inter.mid_flt_price_ivs", "inter", true);

        Assert.assertNull(table.getDatabase());
        Assert.assertEquals("inter", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParsePgTwoPartNameIgnoreCasePrefersConfiguredSpelling() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("INTER.mid_flt_price_ivs", "inter", true);

        Assert.assertNull(table.getDatabase());
        Assert.assertEquals("inter", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }

    @Test
    public void testParsePgFullyQualifiedTableName() {
        CustomSqlTable table = MetricParserUtils.parseCustomSqlTable("rms_hu.inter.mid_flt_price_ivs", "inter", true);

        Assert.assertEquals("rms_hu", table.getDatabase());
        Assert.assertEquals("inter", table.getSchema());
        Assert.assertEquals("mid_flt_price_ivs", table.getTable());
    }
}

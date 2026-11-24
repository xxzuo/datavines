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

public class PostgreSqlDialectTest {

    private final PostgreSqlDialect dialect = new PostgreSqlDialect();

    @Test
    public void testBareTableNameWithoutSchema() {
        // the bare name is resolved by the connection's search_path
        Assert.assertEquals("events", dialect.getFullQualifiedTableName("rms_hu", null, "events", false));
        Assert.assertEquals("events", dialect.getFullQualifiedTableName("rms_hu", null, "events", true));
    }

    @Test
    public void testBareTableNameWithEmptySchema() {
        Assert.assertEquals("events", dialect.getFullQualifiedTableName("rms_hu", "", "events", false));
        Assert.assertEquals("events", dialect.getFullQualifiedTableName("rms_hu", "null", "events", false));
    }

    @Test
    public void testSchemaQualifiedTableName() {
        Assert.assertEquals("other_schema.events", dialect.getFullQualifiedTableName("rms_hu", "other_schema", "events", false));
        Assert.assertEquals("other_schema.events", dialect.getFullQualifiedTableName("rms_hu", "other_schema", "events", true));
    }

    @Test
    public void testPublicSchema() {
        Assert.assertEquals("public.events", dialect.getFullQualifiedTableName("rms_hu", "public", "events", false));
    }

    @Test
    public void testDatabaseNeverPrepended() {
        // database.table would be misread as schema.table by PostgreSQL
        Assert.assertEquals("events", dialect.getFullQualifiedTableName("rms_hu", null, "events", false));
        Assert.assertEquals("inter.events", dialect.getFullQualifiedTableName("other_db", "inter", "events", false));
    }
}

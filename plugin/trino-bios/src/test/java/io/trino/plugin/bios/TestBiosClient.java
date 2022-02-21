/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.bios;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.net.URI;

import static io.trino.plugin.bios.MetadataUtil.CATALOG_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestBiosClient
{
    @Test
    public void testMetadata()
            throws Exception
    {
        BiosConfig config = new BiosConfig().setUrl(new URI("https://load.tieredfractals.com"));
        config.setEmail("dummy@a.com");
        config.setPassword("dummy");
        BiosClient client = new BiosClient(config, CATALOG_CODEC);
        assertEquals(client.getSchemaNames(), ImmutableSet.of("signal", "context"));
        assertEquals(client.getTableNames("signal"), ImmutableSet.of("signal1"));
        assertEquals(client.getTableNames("context"), ImmutableSet.of("context1"));

        BiosTable table = client.getTable("signal", "signal1");
        assertNotNull(table, "table is null");
        assertEquals(table.getName(), "signal1");
        // assertEquals(table.getColumns(), ImmutableList.of(new BiosColumn("dummyString", createUnboundedVarcharType()), new BiosColumn("dummyInt", BIGINT)));
        assertEquals(table.getColumns(), ImmutableList.of(new BiosColumn("dummyString", BIGINT),
                new BiosColumn("dummyInt", BIGINT)));

        table = client.getTable("context", "context1");
        assertNotNull(table, "table is null");
        assertEquals(table.getName(), "context1");
        assertEquals(table.getColumns(), ImmutableList.of(new BiosColumn("dummyString", BIGINT),
                new BiosColumn("dummyInt", BIGINT)));
        // assertEquals(table.getColumns(), ImmutableList.of(new BiosColumn("dummyString", createUnboundedVarcharType()), new BiosColumn("dummyInt", BIGINT)));
    }
}

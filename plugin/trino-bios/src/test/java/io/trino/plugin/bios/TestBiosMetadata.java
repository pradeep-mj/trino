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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static io.trino.plugin.bios.MetadataUtil.CATALOG_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestBiosMetadata
{
    private static final BiosTableHandle TABLE_HANDLE_1 = new BiosTableHandle("signal", "signal1");
    private BiosMetadata metadata;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        BiosConfig config = new BiosConfig().setUrl(new URI("https://load.tieredfractals.com"));
        config.setEmail("dummy@a.com");
        config.setPassword("dummy");
        BiosClient client = new BiosClient(config, CATALOG_CODEC);
        metadata = new BiosMetadata(client);
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableSet.of("context", "signal"));
    }

    @Test
    public void testGetTableHandle()
    {
        assertEquals(metadata.getTableHandle(SESSION, new SchemaTableName("signal", "signal1")),
                TABLE_HANDLE_1);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("signal", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "signal1")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("context", "signal1")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test(enabled = false)
    public void testGetColumnHandles()
    {
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, TABLE_HANDLE_1), ImmutableMap.of(
                "dummyString", new BiosColumnHandle("dummyString"),
                "dummyInt", new BiosColumnHandle("dummyInt")));

        // unknown table
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new BiosTableHandle("unknown", "unknown")))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'unknown.unknown' not found");
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new BiosTableHandle("signal", "unknown")))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'bios.unknown' not found");
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, TABLE_HANDLE_1);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("signal", "signal1"));
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("dummyString", BIGINT),
                // new ColumnMetadata("dummyString", createUnboundedVarcharType()),
                new ColumnMetadata("dummyInt", BIGINT)));

        // unknown tables should produce null
        assertNull(metadata.getTableMetadata(SESSION, new BiosTableHandle("signal", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new BiosTableHandle("unknown", "signal1")));
        assertNull(metadata.getTableMetadata(SESSION, new BiosTableHandle("context", "signal1")));
        assertNull(metadata.getTableMetadata(SESSION, new BiosTableHandle("unknown", "unknown")));
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty())), ImmutableSet.of(
                new SchemaTableName("signal", "signal1"),
                new SchemaTableName("context", "context1")));

        // specific schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("signal"))), ImmutableSet.of(
                new SchemaTableName("signal", "signal1")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("context"))), ImmutableSet.of(
                new SchemaTableName("context", "context1")));

        // unknown schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("unknown"))), ImmutableSet.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(metadata.getColumnMetadata(SESSION, TABLE_HANDLE_1, new BiosColumnHandle(
                "dummyString")),
                new ColumnMetadata("dummyString", BIGINT));
                // new ColumnMetadata("dummyString", createUnboundedVarcharType()));

        // bios connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // BiosTableHandle and BiosColumnHandle passed in.  This is on because
        // it is not possible for the Trino Metadata system to create the handles
        // directly.
    }

    @Test
    public void testCreateTable()
    {
        assertThatThrownBy(() -> metadata.createTable(
                SESSION,
                new ConnectorTableMetadata(
                        new SchemaTableName("signal", "foo"),
                        ImmutableList.of(new ColumnMetadata("dummyString",
                                BIGINT))),
                                // createUnboundedVarcharType()))),
                false))
                .isInstanceOf(TrinoException.class)
                .hasMessage("This connector does not support creating tables");
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testDropTableTable()
    {
        metadata.dropTable(SESSION, TABLE_HANDLE_1);
    }
}

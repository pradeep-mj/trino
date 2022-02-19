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

import java.util.Optional;

import static io.trino.plugin.bios.MetadataUtil.CATALOG_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestBiosMetadata
{
    private static final BiosTableHandle NUMBERS_TABLE_HANDLE = new BiosTableHandle("bios", "numbers");
    private BiosMetadata metadata;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        BiosClient client = new BiosClient(new BiosConfig().setUrl(null), CATALOG_CODEC);
        metadata = new BiosMetadata(client);
    }

    @Test
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION), ImmutableSet.of("bios", "tpch"));
    }

    @Test
    public void testGetTableHandle()
    {
        assertEquals(metadata.getTableHandle(SESSION, new SchemaTableName("bios", "numbers")), NUMBERS_TABLE_HANDLE);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("bios", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "numbers")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test
    public void testGetColumnHandles()
    {
        // known table
        assertEquals(metadata.getColumnHandles(SESSION, NUMBERS_TABLE_HANDLE), ImmutableMap.of(
                "text", new BiosColumnHandle("text", createUnboundedVarcharType(), 0),
                "value", new BiosColumnHandle("value", BIGINT, 1)));

        // unknown table
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new BiosTableHandle("unknown", "unknown")))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'unknown.unknown' not found");
        assertThatThrownBy(() -> metadata.getColumnHandles(SESSION, new BiosTableHandle("bios", "unknown")))
                .isInstanceOf(TableNotFoundException.class)
                .hasMessage("Table 'bios.unknown' not found");
    }

    @Test
    public void getTableMetadata()
    {
        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, NUMBERS_TABLE_HANDLE);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("bios", "numbers"));
        assertEquals(tableMetadata.getColumns(), ImmutableList.of(
                new ColumnMetadata("text", createUnboundedVarcharType()),
                new ColumnMetadata("value", BIGINT)));

        // unknown tables should produce null
        assertNull(metadata.getTableMetadata(SESSION, new BiosTableHandle("unknown", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new BiosTableHandle("bios", "unknown")));
        assertNull(metadata.getTableMetadata(SESSION, new BiosTableHandle("unknown", "numbers")));
    }

    @Test
    public void testListTables()
    {
        // all schemas
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.empty())), ImmutableSet.of(
                new SchemaTableName("bios", "numbers"),
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));

        // specific schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("bios"))), ImmutableSet.of(
                new SchemaTableName("bios", "numbers")));
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("tpch"))), ImmutableSet.of(
                new SchemaTableName("tpch", "orders"),
                new SchemaTableName("tpch", "lineitem")));

        // unknown schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("unknown"))), ImmutableSet.of());
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(metadata.getColumnMetadata(SESSION, NUMBERS_TABLE_HANDLE, new BiosColumnHandle("text", createUnboundedVarcharType(), 0)),
                new ColumnMetadata("text", createUnboundedVarcharType()));

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
                        new SchemaTableName("bios", "foo"),
                        ImmutableList.of(new ColumnMetadata("text", createUnboundedVarcharType()))),
                false))
                .isInstanceOf(TrinoException.class)
                .hasMessage("This connector does not support creating tables");
    }

    @Test(expectedExceptions = TrinoException.class)
    public void testDropTableTable()
    {
        metadata.dropTable(SESSION, NUMBERS_TABLE_HANDLE);
    }
}

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
import io.trino.spi.connector.ColumnMetadata;
import org.testng.annotations.Test;

import static io.trino.plugin.bios.MetadataUtil.TABLE_CODEC;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public class TestBiosTable
{
    private final BiosTable biosTable = new BiosTable(BiosTableKind.SIGNAL, "tableName");

    @Test
    public void testColumnMetadata()
    {
        assertEquals(biosTable.getColumnsMetadata(), ImmutableList.of(
                new ColumnMetadata("dummyString", BIGINT),
                // new ColumnMetadata("a", createUnboundedVarcharType()),
                new ColumnMetadata("dummyInt", BIGINT)));
    }

    @Test
    public void testRoundTrip()
    {
        String json = TABLE_CODEC.toJson(biosTable);
        BiosTable biosTableCopy = TABLE_CODEC.fromJson(json);

        assertEquals(biosTableCopy.getName(), biosTable.getName());
        assertEquals(biosTableCopy.getColumns(), biosTable.getColumns());
    }
}

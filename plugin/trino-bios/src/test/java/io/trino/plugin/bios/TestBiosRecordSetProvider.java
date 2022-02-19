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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestBiosRecordSetProvider
{
    @Test
    public void testGetRecordSet()
    {
        ConnectorTableHandle tableHandle = new BiosTableHandle("schema", "table");
        BiosRecordSetProvider recordSetProvider = new BiosRecordSetProvider();
        RecordSet recordSet = recordSetProvider.getRecordSet(BiosTransactionHandle.INSTANCE,
                SESSION, new BiosSplit("testTable"), tableHandle, ImmutableList.of(
                    new BiosColumnHandle("text", createUnboundedVarcharType(), 0),
                    new BiosColumnHandle("value", BIGINT, 1)));
        assertNotNull(recordSet, "recordSet is null");

        RecordCursor cursor = recordSet.cursor();
        assertNotNull(cursor, "cursor is null");

        Map<String, Long> data = new LinkedHashMap<>();
        while (cursor.advanceNextPosition()) {
            data.put(cursor.getSlice(0).toStringUtf8(), cursor.getLong(1));
        }
        assertEquals(data, ImmutableMap.<String, Long>builder()
                .put("ten", 10L)
                .put("eleven", 11L)
                .put("twelve", 12L)
                .buildOrThrow());
    }

    //
    // Start http server for testing
    //

    @BeforeClass
    public void setUp()
    {
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
    }
}

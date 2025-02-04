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

import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestBiosTableHandle
{
    private final BiosTableHandle tableHandle = new BiosTableHandle("schemaName", "tableName");

    @Test(enabled = false)
    public void testJsonRoundTrip()
    {
        JsonCodec<BiosTableHandle> codec = jsonCodec(BiosTableHandle.class);
        String json = codec.toJson(tableHandle);
        BiosTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(new BiosTableHandle("schema", "table"), new BiosTableHandle("schema", "table"))
                .addEquivalentGroup(new BiosTableHandle("schemaX", "table"), new BiosTableHandle("schemaX", "table"))
                .addEquivalentGroup(new BiosTableHandle("schema", "tableX"), new BiosTableHandle("schema", "tableX"))
                .check();
    }
}

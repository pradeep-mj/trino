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
import io.airlift.json.JsonCodec;
import io.trino.spi.HostAddress;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestBiosSplit
{
    private final BiosSplit split = new BiosSplit("http://127.0.0.1/test.file");

    @Test
    public void testAddresses()
    {
        // http split with default port
        BiosSplit httpSplit = new BiosSplit("http://bios.com/bios");
        assertEquals(httpSplit.getAddresses(), ImmutableList.of(HostAddress.fromString("bios.com")));
        assertEquals(httpSplit.isRemotelyAccessible(), true);

        // http split with custom port
        httpSplit = new BiosSplit("http://bios.com:8080/bios");
        assertEquals(httpSplit.getAddresses(), ImmutableList.of(HostAddress.fromParts("bios.com", 8080)));
        assertEquals(httpSplit.isRemotelyAccessible(), true);

        // http split with default port
        BiosSplit httpsSplit = new BiosSplit("https://bios.com/bios");
        assertEquals(httpsSplit.getAddresses(), ImmutableList.of(HostAddress.fromString("bios.com")));
        assertEquals(httpsSplit.isRemotelyAccessible(), true);

        // http split with custom port
        httpsSplit = new BiosSplit("https://bios.com:8443/bios");
        assertEquals(httpsSplit.getAddresses(), ImmutableList.of(HostAddress.fromParts("bios.com", 8443)));
        assertEquals(httpsSplit.isRemotelyAccessible(), true);
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<BiosSplit> codec = jsonCodec(BiosSplit.class);
        String json = codec.toJson(split);
        BiosSplit copy = codec.fromJson(json);
        assertEquals(copy.getTableName(), split.getTableName());

        assertEquals(copy.getAddresses(), ImmutableList.of(HostAddress.fromString("127.0.0.1")));
        assertEquals(copy.isRemotelyAccessible(), true);
    }
}

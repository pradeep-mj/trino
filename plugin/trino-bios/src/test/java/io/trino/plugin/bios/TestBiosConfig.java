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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestBiosConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BiosConfig.class)
                .setUrl(null)
                .setEmail(null)
                .setPassword(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("bios.url", "https://load.tieredfractals.com")
                .put("bios.email", "test_user@abc.com")
                .put("bios.password", "test_password")
                .buildOrThrow();

        BiosConfig expected = new BiosConfig()
                .setUrl(URI.create("https://load.tieredfractals.com"))
                .setEmail("test_user@abc.com")
                .setPassword("test_password");

        assertFullMapping(properties, expected);
    }
}

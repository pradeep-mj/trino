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

import io.trino.spi.security.PasswordAuthenticator;
import io.trino.spi.security.PasswordAuthenticatorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class BiosPasswordAuthenticatorFactory
        implements PasswordAuthenticatorFactory
{
    @Override
    public String getName()
    {
        return "biosAuthenticator";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config)
    {
        requireNonNull(config, "config is null");

        return new BiosPasswordAuthenticator(config);
    }
}

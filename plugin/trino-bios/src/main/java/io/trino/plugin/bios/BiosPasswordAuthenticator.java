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

import io.airlift.log.Logger;
import io.trino.spi.security.PasswordAuthenticator;

import java.security.Principal;
import java.util.Map;

public class BiosPasswordAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger logger = Logger.get(BiosPasswordAuthenticator.class);

    private Map<String, String> config;

    public BiosPasswordAuthenticator(Map<String, String> config)
    {
        logger.debug("BiosPasswordAuthenticator initialized");
        this.config = config;
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        logger.debug("createAuthenticatedPrincipal %s %s", user, password);
        return new BiosPrincipal(user, password);
    }
}

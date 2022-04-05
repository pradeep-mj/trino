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
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.trino.spi.TrinoException;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.PasswordAuthenticator;

import java.net.URI;
import java.security.Principal;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class BiosPasswordAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger logger = Logger.get(BiosPasswordAuthenticator.class);

    private final URI url;
    private final String tenant;

    public BiosPasswordAuthenticator(Map<String, String> config)
    {
        logger.debug("BiosPasswordAuthenticator initialized; config %s", config);
        final var urlString = config.get("biosAuthenticator.url");
        if (urlString == null) {
            throw new TrinoException(GENERIC_USER_ERROR,
                    "biosAuthenticator.url not provided in password-authenticator.properties");
        }
        this.url = URI.create(urlString);
        tenant = config.get("biosAuthenticator.tenant");
        if (tenant == null) {
            throw new TrinoException(GENERIC_USER_ERROR,
                    "biosAuthenticator.tenant not provided in password-authenticator.properties");
        }
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        Session session;
        try {
            logger.debug("Authenticating at %s: %s ...", url, user);
            session = Bios.newSession(url.getHost(), 443)
                    .user(user)
                    .password(password)
                    .sslCertFile(null)
                    .connect();
            var tenantConfig = session.getTenant(false, false);
            if (!tenantConfig.getName().equals(tenant)) {
                logger.warn("Valid user from a different tenant attempted authenticating. "
                                + "Configured tenant: %s, authenticated user: %s, user's tenant: %s",
                        tenant, user, tenantConfig.getName());
                throw new AccessDeniedException("Invalid tenant access");
            }
            logger.debug("Authentication succeeded");
            return new BiosPrincipal(user, session);
        }
        catch (BiosClientException e) {
            logger.debug("Authentication failed: %s", e);
            throw new AccessDeniedException("Invalid bios user email or password");
        }
    }
}

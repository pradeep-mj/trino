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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.collect.cache.SafeCaches;
import io.trino.spi.TrinoException;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.PasswordAuthenticator;

import java.net.URI;
import java.security.Principal;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;

public class BiosPasswordAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger logger = Logger.get(BiosPasswordAuthenticator.class);

    private final URI url;
    private final String tenant;
    private final NonEvictableLoadingCache<UserAndPassword, Session> sessionCache;

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

        sessionCache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder()
                        .maximumSize(1000)
                        .expireAfterWrite(60 * 60, TimeUnit.SECONDS),
                new CacheLoader<>() {
                    @Override
                    public Session load(final UserAndPassword userAndPassword)
                    {
                        Session session;
                        try {
                            logger.debug("----> Authenticating at %s: %s ...", url,
                                    userAndPassword.user);
                            session = Bios.newSession(url.getHost(), 443)
                                    .user(userAndPassword.user)
                                    .password(userAndPassword.password)
                                    .sslCertFile(null)
                                    .connect();
                            var tenantConfig = session.getTenant(false, false);
                            if (!tenantConfig.getName().equals(tenant)) {
                                logger.warn("<---- Valid user from a different tenant attempted "
                                                + "authenticating. "
                                                + "Configured tenant: %s, authenticated user: %s, user's tenant: %s",
                                        tenant, userAndPassword.user, tenantConfig.getName());
                                throw new AccessDeniedException("Invalid tenant access");
                            }
                            logger.debug("<---- Authentication succeeded");
                        }
                        catch (BiosClientException e) {
                            logger.debug("Authentication failed: %s", e);
                            throw new AccessDeniedException("Invalid bios user email or password");
                        }
                        return session;
                    }
                });
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        logger.debug("createAuthenticatedPrincipal %s ...", user);

        Session session;
        try {
            // This will throw AccessDeniedException if authentication is not successful.
            session = sessionCache.getUnchecked(new UserAndPassword(user, password));
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof AccessDeniedException) {
                throw (AccessDeniedException) e.getCause();
            }
            else {
                throw e;
            }
        }
        return new BiosPrincipal(user, session);
    }

    private static class UserAndPassword
    {
        private final String user;
        private final String password;

        protected UserAndPassword(String user, String password)
        {
            this.user = user;
            this.password = password;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(user, password);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }

            UserAndPassword other = (UserAndPassword) obj;
            return Objects.equals(this.user, other.user) &&
                    Objects.equals(this.password, other.password);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("user", user)
                    .add("password", password)
                    .toString();
        }
    }
}

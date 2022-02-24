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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.trino.spi.type.Type;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class BiosClient
{
    private static final Logger logger = Logger.get(BiosClient.class);
    private static final Map<String, Type> biosTypeMap = new HashMap<>();

    static {
        biosTypeMap.put("integer", BIGINT);
        biosTypeMap.put("boolean", BOOLEAN);
        biosTypeMap.put("string", VARCHAR);
        biosTypeMap.put("decimal", DOUBLE);
    }

    private final URI url;
    private final String email;
    private final String password;
    private final Supplier<Session> session;

    @Inject
    public BiosClient(BiosConfig config, JsonCodec<Map<String, List<BiosTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        url = config.getUrl();
        requireNonNull(url, "url is null");

        email = config.getEmail();
        password = config.getPassword();
        checkArgument(!isNullOrEmpty(email), "email is null");
        checkArgument(!isNullOrEmpty(password), "password is null");

        session = Suppliers.memoize(sessionSupplier(url, email, password));
    }

    private static Supplier<Session> sessionSupplier(URI url, String email, String password)
    {
        return () -> {
            Session session;
            try {
                logger.debug("sessionSupplier: %s (%s), %s, %s", url.toString(), url.getHost(),
                        email,
                        password);
                session = Bios.newSession(url.getHost(), 443)
                        .user(email)
                        .password(password)
                        .sslCertFile(null)
                        .connect();
                logger.debug("sessionSupplier: done");
                return session;
            }
            catch (BiosClientException e) {
                throw new RuntimeException(e.toString());
            }
        };
    }

    public List<String> getSchemaNames()
    {
        return ImmutableList.of("context", "signal");
    }

    public Set<String> getTableNames(String schemaName)
    {
        logger.debug("getTableNames: %s", schemaName);
        requireNonNull(schemaName, "schemaName is null");

        TenantConfig tenantConfig;
        try {
            tenantConfig = session.get().getTenant(true, true);
        }
        catch (BiosClientException e) {
            throw new RuntimeException(e.toString());
        }
        List<String> tableNames = new ArrayList<>();

        if (schemaName.equals("signal")) {
            for (SignalConfig signalConfig : tenantConfig.getSignals()) {
                tableNames.add(signalConfig.getName());
            }
        }
        else if (schemaName.equals("context")) {
            for (ContextConfig contextConfig : tenantConfig.getContexts()) {
                tableNames.add(contextConfig.getName());
            }
        }
        logger.debug("getTableNames: %s", tableNames.toString());
        return ImmutableSet.copyOf(tableNames);
    }

    public BiosTable getTable(String schemaName, String tableName)
    {
        // logger.debug("getTable: %s.%s", schemaName, tableName);
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");

        TenantConfig tenantConfig;
        try {
            tenantConfig = session.get().getTenant(true, true);
        }
        catch (BiosClientException e) {
            throw new RuntimeException(e.toString());
        }

        BiosTableHandle tableHandle = new BiosTableHandle(schemaName, tableName);
        List<BiosColumnHandle> columns = new ArrayList<>();
        BiosTable table = null;

        if (schemaName.equals("signal")) {
            for (SignalConfig signalConfig : tenantConfig.getSignals()) {
                if (!tableName.equalsIgnoreCase(signalConfig.getName())) {
                    continue;
                }
                for (AttributeConfig attributeConfig : signalConfig.getAttributes()) {
                    String columnName = attributeConfig.getName();
                    Type columnType = biosTypeMap.get(attributeConfig.getType().name().toLowerCase(Locale.getDefault()));
                    BiosColumnHandle columnHandle = new BiosColumnHandle(columnName, columnType,
                            BiosTableKind.SIGNAL, false);
                    columns.add(columnHandle);
                }
                table = new BiosTable(BiosTableKind.SIGNAL, tableHandle, columns);
            }
        }
        else if (schemaName.equals("context")) {
            for (ContextConfig contextConfig : tenantConfig.getContexts()) {
                if (!tableName.equalsIgnoreCase(contextConfig.getName())) {
                    continue;
                }
                boolean isFirstAttribute = true;
                for (AttributeConfig attributeConfig : contextConfig.getAttributes()) {
                    String columnName = attributeConfig.getName();
                    Type columnType = biosTypeMap.get(attributeConfig.getType().name().toLowerCase(Locale.getDefault()));
                    BiosColumnHandle columnHandle = new BiosColumnHandle(columnName, columnType,
                            BiosTableKind.CONTEXT, isFirstAttribute);
                    isFirstAttribute = false;
                    columns.add(columnHandle);
                }
                table = new BiosTable(BiosTableKind.SIGNAL, tableHandle, columns);
            }
        }

        return table;
    }

    public URI getUrl()
    {
        return url;
    }

    public Session getSession()
    {
        return session.get();
    }
}

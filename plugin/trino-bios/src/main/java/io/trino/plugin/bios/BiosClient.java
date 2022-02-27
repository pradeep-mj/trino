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
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.isima.bios.sdk.errors.BiosClientError.SESSION_EXPIRED;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class BiosClient
{
    public static final String SIGNAL_TIMESTAMP_COLUMN = "__eventTimestamp";
    public static final String CONTEXT_TIMESTAMP_COLUMN = "__upsertTimestamp";

    private static final Logger logger = Logger.get(BiosClient.class);
    private static final Map<String, Type> biosTypeMap = new HashMap<>();

    static {
        biosTypeMap.put("integer", BIGINT);
        biosTypeMap.put("boolean", BOOLEAN);
        biosTypeMap.put("string", VARCHAR);
        biosTypeMap.put("decimal", DOUBLE);
    }

    private final BiosConfig biosConfig;
    private Supplier<Session> session;

    @Inject
    public BiosClient(BiosConfig config, JsonCodec<Map<String, List<BiosTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        requireNonNull(config.getUrl(), "url is null");
        checkArgument(!isNullOrEmpty(config.getEmail()), "email is null");
        checkArgument(!isNullOrEmpty(config.getPassword()), "password is null");

        this.biosConfig = config;

        session = Suppliers.memoize(sessionSupplier(config));
    }

    private static Supplier<Session> sessionSupplier(BiosConfig biosConfig)
    {
        return () -> {
            Session session;
            try {
                logger.debug("sessionSupplier: %s (%s), %s, %s", biosConfig.getUrl().toString(),
                        biosConfig.getUrl().getHost(),
                        biosConfig.getEmail(),
                        biosConfig.getPassword());
                session = Bios.newSession(biosConfig.getUrl().getHost(), 443)
                        .user(biosConfig.getEmail())
                        .password(biosConfig.getPassword())
                        .sslCertFile(null)
                        .connect();
                logger.debug("sessionSupplier: done");
                return session;
            }
            catch (BiosClientException e) {
                // Cannot call handleException() here because it may cause recursion.
                throw new RuntimeException(e.toString());
            }
        };
    }

    /**
     * This method always throws a RuntimeException.
     * For some input exceptions it may do some additional handling before throwing the exception.
     * Calling code can assume that an exception will be thrown after this method is called,
     * e.g. for accessing variables initialized inside a try/catch block.
     */
    public void handleException(Exception e)
    {
        if (e instanceof BiosClientException) {
            BiosClientException biosClientException = (BiosClientException) e;
            if (biosClientException.getCode().equals(SESSION_EXPIRED)) {
                logger.debug("Session expired: %s \n\n Attempting to create a new session...",
                        biosClientException.toString());
                session = Suppliers.memoize(sessionSupplier(biosConfig));
                session.get();
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "bi(OS) session expired and "
                        + "has been reestablished; please retry the query.");
            }
            else {
                throw new RuntimeException(biosClientException.toString());
            }
        }
        else {
            throw new RuntimeException(e.toString());
        }
    }

    public List<String> getSchemaNames()
    {
        return ImmutableList.of("context", "signal");
    }

    public Set<String> getTableNames(String schemaName)
    {
        // logger.debug("getTableNames: %s", schemaName);
        requireNonNull(schemaName, "schemaName is null");

        TenantConfig tenantConfig = null;
        try {
            tenantConfig = session.get().getTenant(true, true);
        }
        catch (BiosClientException e) {
            handleException(e);
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

        TenantConfig tenantConfig = null;
        try {
            tenantConfig = session.get().getTenant(true, true);
        }
        catch (BiosClientException e) {
            handleException(e);
        }

        BiosTableHandle tableHandle = new BiosTableHandle(schemaName, tableName);
        BiosTableKind kind = null;
        List<AttributeConfig> attributes = null;
        List<BiosColumnHandle> columns = new ArrayList<>();
        BiosTable table = null;
        String defaultValue = null;
        String timestampColumnName = null;

        if (schemaName.equals("signal")) {
            kind = BiosTableKind.SIGNAL;
            timestampColumnName = SIGNAL_TIMESTAMP_COLUMN;
            for (SignalConfig signalConfig : tenantConfig.getSignals()) {
                if (!tableName.equalsIgnoreCase(signalConfig.getName())) {
                    continue;
                }
                attributes = signalConfig.getAttributes();
                break;
            }
        }
        else if (schemaName.equals("context")) {
            kind = BiosTableKind.CONTEXT;
            timestampColumnName = CONTEXT_TIMESTAMP_COLUMN;
            for (ContextConfig contextConfig : tenantConfig.getContexts()) {
                if (!tableName.equalsIgnoreCase(contextConfig.getName())) {
                    continue;
                }
                attributes = contextConfig.getAttributes();
                break;
            }
        }
        if (attributes == null) {
            return null;
        }

        boolean isFirstAttribute = true;
        for (AttributeConfig attributeConfig : attributes) {
            String columnName = attributeConfig.getName();
            Type columnType = biosTypeMap.get(attributeConfig.getType().name().toLowerCase(Locale.getDefault()));
            if (attributeConfig.getDefaultValue() != null) {
                defaultValue = attributeConfig.getDefaultValue().asString();
            }
            else {
                defaultValue = null;
            }
            BiosColumnHandle columnHandle = new BiosColumnHandle(columnName, columnType,
                    defaultValue, (kind == BiosTableKind.CONTEXT) && isFirstAttribute, false);
            isFirstAttribute = false;
            columns.add(columnHandle);
        }
        columns.add(new BiosColumnHandle(timestampColumnName, TIMESTAMP_MICROS, null, false,
                true));

        table = new BiosTable(tableHandle, columns);

        return table;
    }

    public BiosConfig getBiosConfig()
    {
        return biosConfig;
    }

    public Session getSession()
    {
        return session.get();
    }
}

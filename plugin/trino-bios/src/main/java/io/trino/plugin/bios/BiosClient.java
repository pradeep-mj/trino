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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.models.isql.ISqlStatement;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.collect.cache.SafeCaches;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.isima.bios.models.isql.WhereClause.keys;
import static io.isima.bios.sdk.errors.BiosClientError.SESSION_EXPIRED;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class BiosClient
{
    public static final String SIGNAL_TIMESTAMP_COLUMN = "__eventTimestamp";
    public static final String CONTEXT_TIMESTAMP_COLUMN = "__upsertTimestamp";
    public static final String SIGNAL_TIME_EPOCH_MS_COLUMN = "__eventTimeEpochMs";
    public static final String CONTEXT_TIME_EPOCH_MS_COLUMN = "__upsertTimeEpochMs";
    public static final String RAW_SIGNAL_TABLE_NAME_SUFFIX = "_raw";

    private static final Logger logger = Logger.get(BiosClient.class);
    private static final Map<String, Type> biosTypeMap = new HashMap<>();

    static {
        biosTypeMap.put("integer", BIGINT);
        biosTypeMap.put("boolean", BOOLEAN);
        biosTypeMap.put("string", VARCHAR);
        biosTypeMap.put("decimal", DOUBLE);
        biosTypeMap.put("blob", VARBINARY);
    }

    private final BiosConfig biosConfig;
    private Supplier<Session> session;
    private Supplier<TenantConfig> tenantConfig;
    private NonEvictableLoadingCache<BiosQuery, ISqlResponse> dataCache;

    @Inject
    public BiosClient(BiosConfig config)
    {
        requireNonNull(config, "config is null");

        requireNonNull(config.getUrl(), "url is null");
        checkArgument(!isNullOrEmpty(config.getEmail()), "email is null");
        checkArgument(!isNullOrEmpty(config.getPassword()), "password is null");

        this.biosConfig = config;

        session = Suppliers.memoize(sessionSupplier(config));
        tenantConfig = Suppliers.memoizeWithExpiration(tenantConfigSupplier(this),
                config.getMetadataCacheSeconds(), TimeUnit.SECONDS);
        tenantConfig.get();

        dataCache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumWeight(config.getDataCacheSizeInRows())
                .weigher(new Weigher<BiosQuery, ISqlResponse>() {
                    @Override
                    public int weigh(BiosQuery query, ISqlResponse response)
                    {
                        int numRows = 0;
                        numRows += response.getRecords().size();
                        for (var window : response.getDataWindows()) {
                            numRows += window.getRecords().size();
                        }
                        return numRows;
                    }
                })
                .expireAfterWrite(config.getDataCacheSeconds(), TimeUnit.SECONDS),
                    new CacheLoader<BiosQuery, ISqlResponse>() {
                        @Override
                        public ISqlResponse load(final BiosQuery query)
                        {
                            return executeInternal(query);
                        }
                    });
        // Execute one query to initialize bios SDK metrics.
        execute(new BiosQuery("raw_signal", addRawSuffix("_requests"),
                System.currentTimeMillis(), -60000L, null));
    }

    /**
     * This method always throws a RuntimeException.
     * For some input exceptions it may do some additional handling before throwing the exception.
     * Calling code can assume that an exception will be thrown after this method is called,
     * e.g. for accessing variables initialized inside a try/catch block.
     */
    private void handleException(Exception e)
    {
        logger.debug("bi(OS) got exception: %s", e.toString());
        if (e instanceof BiosClientException) {
            BiosClientException biosClientException = (BiosClientException) e;
            if (biosClientException.getCode().equals(SESSION_EXPIRED)) {
                logger.debug("Session expired: \n\n Attempting to create a new session...");
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

    private static Supplier<TenantConfig> tenantConfigSupplier(final BiosClient client)
    {
        return () -> {
            TenantConfig tenantConfig = null;
            try {
                logger.debug("----------> bios network request: getTenant");
                tenantConfig = client.getSession().getTenant(true, true);
            }
            catch (BiosClientException e) {
                client.handleException(e);
            }
            logger.debug("<---------- bios network response: getTenant %s returned %d signals, %d "
                            + "contexts",
                    tenantConfig.getName(), tenantConfig.getSignals().size(),
                    tenantConfig.getContexts().size());
            return tenantConfig;
        };
    }

    private Long floor(Long toBeFloored, long divisor)
    {
        if (toBeFloored == null) {
            return null;
        }
        return divisor * (long) (toBeFloored / divisor);
    }

    public ISqlResponse execute(BiosQuery query)
    {
        // Normalize query parameters for efficiency.
        // 1. Align time range.
        query.setTimeRangeStart(floor(query.getTimeRangeStart(),
                biosConfig.getDataAlignmentSeconds() * 1000));
        // 2. For raw signals, get all attributes so that we don't have many queries with different
        //      subsets of attributes.
        if (query.getTableKind() == BiosTableKind.RAW_SIGNAL) {
            query.setAttributes(null);
        }

        return dataCache.getUnchecked(query);
    }

    private ISqlResponse executeInternal(BiosQuery query)
    {
        ISqlStatement isqlStatement;

        if (query.getTableKind() == BiosTableKind.CONTEXT) {
            // Contexts only support listing the primary key attribute directly.
            // First get all the primary key values, and then issue a second query to get
            // all the attributes for each of those keys.

            var columns = getColumnHandles(query.getSchemaName(), query.getTableName());
            String keyColumnName = columns.get(0).getColumnName();

            ISqlStatement preliminaryStatement = ISqlStatement.select(keyColumnName)
                    .fromContext(query.getUnderlyingTableName())
                    .build();

            ISqlResponse preliminaryResponse = execute(preliminaryStatement);
            String[] keyValues = preliminaryResponse.getRecords().stream()
                    .map(r -> r.getAttribute(keyColumnName).asString())
                    .toArray(String[]::new);

            isqlStatement = ISqlStatement.select()
                    .fromContext(query.getUnderlyingTableName())
                    .where(keys().in((java.lang.Object) keyValues))
                    .build();
        }
        else {
            var partialStatement = ISqlStatement.select();
            if (query.getAttributes() != null) {
                partialStatement = ISqlStatement.select(query.getAttributes());
            }
            isqlStatement = partialStatement
                    .from(query.getUnderlyingTableName())
                    .timeRange(query.getTimeRangeStart(), query.getTimeRangeDelta())
                    .build();
        }

        logger.debug("----------> bios network request: statement %s", query);
        ISqlResponse response = execute(isqlStatement);
        long firstWindowRecords = 0;
        if (response.getDataWindows().size() > 0) {
            firstWindowRecords = response.getDataWindows().get(0).getRecords().size();
        }
        logger.debug("<---------- bios network response: statement returned %d records, %d windows "
                        + "with %d records in first window",
                response.getRecords().size(), response.getDataWindows().size(), firstWindowRecords);

        return response;
    }

    private ISqlResponse execute(ISqlStatement statement)
    {
        ISqlResponse response = null;
        try {
            response = session.get().execute(statement);
        }
        catch (BiosClientException e) {
            handleException(e);
        }
        return response;
    }

    public List<String> getSchemaNames()
    {
        return ImmutableList.of("context", "signal", "raw_signal");
    }

    public static String addRawSuffix(String tableName)
    {
        return tableName + RAW_SIGNAL_TABLE_NAME_SUFFIX;
    }

    public static String removeRawSuffix(String tableName)
    {
        if (!tableName.endsWith(RAW_SIGNAL_TABLE_NAME_SUFFIX)) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "bi(OS) got invalid raw signal table"
                    + " name: " + tableName + "; expected it to end with: " + RAW_SIGNAL_TABLE_NAME_SUFFIX);
        }
        return tableName.substring(0, tableName.length() - RAW_SIGNAL_TABLE_NAME_SUFFIX.length());
    }

    public Set<String> getTableNames(String schemaName)
    {
        // logger.debug("getTableNames: %s", schemaName);
        requireNonNull(schemaName, "schemaName is null");

        List<String> tableNames = new ArrayList<>();

        switch (schemaName) {
            case "signal":
                for (SignalConfig signalConfig : tenantConfig.get().getSignals()) {
                    tableNames.add(signalConfig.getName());
                }
                break;
            case "raw_signal":
                for (SignalConfig signalConfig : tenantConfig.get().getSignals()) {
                    tableNames.add(addRawSuffix(signalConfig.getName()));
                }
                break;
            case "context":
                for (ContextConfig contextConfig : tenantConfig.get().getContexts()) {
                    tableNames.add(contextConfig.getName());
                }
                break;
        }
        logger.debug("getTableNames: %s", tableNames.toString());
        return ImmutableSet.copyOf(tableNames);
    }

    public BiosTableHandle getTableHandle(String schemaName, String tableName)
    {
        return new BiosTableHandle(schemaName, tableName);
    }

    public List<BiosColumnHandle> getColumnHandles(String schemaName, String tableName)
    {
        // logger.debug("getColumnHandles: %s.%s", schemaName, tableName);
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");

        BiosTableKind kind = null;
        List<AttributeConfig> attributes = null;
        ImmutableList.Builder<BiosColumnHandle> columns = ImmutableList.builder();
        String timestampColumnName = null;
        String epochColumnName = null;
        String defaultValue = null;

        switch (schemaName) {
            case "signal":
            case "raw_signal":
                final String underlyingTableName;
                if (schemaName.equals("signal")) {
                    kind = BiosTableKind.SIGNAL;
                    underlyingTableName = tableName;
                }
                else {
                    kind = BiosTableKind.RAW_SIGNAL;
                    underlyingTableName = removeRawSuffix(tableName);
                }
                timestampColumnName = SIGNAL_TIMESTAMP_COLUMN;
                epochColumnName = SIGNAL_TIME_EPOCH_MS_COLUMN;
                for (SignalConfig signalConfig : tenantConfig.get().getSignals()) {
                    if (!underlyingTableName.equalsIgnoreCase(signalConfig.getName())) {
                        continue;
                    }
                    attributes = signalConfig.getAttributes();
                    break;
                }
                break;
            case "context":
                kind = BiosTableKind.CONTEXT;
                timestampColumnName = CONTEXT_TIMESTAMP_COLUMN;
                epochColumnName = CONTEXT_TIME_EPOCH_MS_COLUMN;
                for (ContextConfig contextConfig : tenantConfig.get().getContexts()) {
                    if (!tableName.equalsIgnoreCase(contextConfig.getName())) {
                        continue;
                    }
                    attributes = contextConfig.getAttributes();
                    break;
                }
                break;
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
        columns.add(new BiosColumnHandle(epochColumnName, BIGINT, null, false, true));

        return columns.build();
    }

    public BiosConfig getBiosConfig()
    {
        return biosConfig;
    }

    private Session getSession()
    {
        return session.get();
    }
}

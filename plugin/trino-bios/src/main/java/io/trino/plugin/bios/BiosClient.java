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
import io.isima.bios.models.isql.Metric;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.trino.collect.cache.NonEvictableLoadingCache;
import io.trino.collect.cache.SafeCaches;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.isima.bios.models.isql.Metric.count;
import static io.isima.bios.models.isql.Metric.max;
import static io.isima.bios.models.isql.Metric.min;
import static io.isima.bios.models.isql.Metric.sum;
import static io.isima.bios.models.isql.WhereClause.keys;
import static io.isima.bios.models.isql.Window.tumbling;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("NullableProblems")
public class BiosClient
{
    public static final String RAW_SIGNAL_TABLE_NAME_SUFFIX = "_raw";
    public static final String VIRTUAL_PREFIX = "__";
    public static final String PARAMETER_PREFIX = VIRTUAL_PREFIX + "param_";

    public static final String COLUMN_PARAM_WINDOW_SIZE_SECONDS = PARAMETER_PREFIX + "window_size_seconds";
    public static final String COLUMN_PARAM_QUERY_PERIOD_SECONDS = PARAMETER_PREFIX + "query_period_seconds";
    public static final String COLUMN_PARAM_QUERY_PERIOD_OFFSET_SECONDS = PARAMETER_PREFIX + "query_period_offset_seconds";

    public static final String COLUMN_SIGNAL_TIMESTAMP = VIRTUAL_PREFIX + "event_timestamp";
    public static final String COLUMN_CONTEXT_TIMESTAMP = VIRTUAL_PREFIX + "upsert_timestamp";
    public static final String COLUMN_SIGNAL_TIME_EPOCH_MS = VIRTUAL_PREFIX + "event_time_epoch_ms";
    public static final String COLUMN_CONTEXT_TIME_EPOCH_MS = VIRTUAL_PREFIX + "upsert_time_epoch_ms";
    public static final String COLUMN_WINDOW_BEGIN_EPOCH = VIRTUAL_PREFIX + "window_begin_epoch";
    public static final String COLUMN_WINDOW_BEGIN_TIMESTAMP = VIRTUAL_PREFIX + "window_begin_timestamp";

    public static final String MAX_WINDOW_BEGIN_EPOCH = "max(" + COLUMN_WINDOW_BEGIN_EPOCH + ")";
    public static final String MAX_WINDOW_BEGIN_TIMESTAMP = "max(" + COLUMN_WINDOW_BEGIN_TIMESTAMP + ")";

    private static final Logger logger = Logger.get(BiosClient.class);
    private static final Map<String, Type> biosTypeMap = new HashMap<>();
    private static final Set<String> aggregates;
    public static final String SCHEMA_CONTEXTS = "contexts";
    public static final String SCHEMA_SIGNALS = "signals";
    public static final String SCHEMA_RAW_SIGNALS = "raw_signals";

    static {
        biosTypeMap.put("integer", BIGINT);
        biosTypeMap.put("boolean", BOOLEAN);
        biosTypeMap.put("string", VARCHAR);
        biosTypeMap.put("decimal", DOUBLE);
        biosTypeMap.put("blob", VARBINARY);

        aggregates = new HashSet<>(Arrays.asList("sum", "count", "min", "max",
                "avg", "variance", "stddev", "skewness", "kurtosis", "sum2", "sum3", "sum4",
                "median", "p0_01", "p0_1", "p1", "p10", "p25", "p50", "p75", "p90", "p99", "p99_9", "p99_99",
                "distinctcount", "dclb1", "dcub1", "dclb2", "dcub2", "dclb3", "dcub3",
                "numsamples", "samplingfraction"));
    }

    private final BiosConfig biosConfig;
    private Supplier<Session> session;
    private final Supplier<TenantConfig> tenantConfig;
    private final NonEvictableLoadingCache<BiosQuery, ISqlResponse> contextCache;
    private final NonEvictableLoadingCache<BiosQuery, ISqlResponse> rawSignalCache;
    private final NonEvictableLoadingCache<BiosQuery, ISqlResponse> featureCache;

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

        contextCache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumWeight(config.getContextCacheSizeInRows())
                .weigher((Weigher<BiosQuery, ISqlResponse>) (query, response) -> getTotalRows(response))
                .expireAfterWrite(config.getContextCacheSeconds(), TimeUnit.SECONDS),
                    new CacheLoader<>() {
                        @Override
                        public ISqlResponse load(final BiosQuery query)
                        {
                            return executeInternal(query);
                        }
                    });

        rawSignalCache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumWeight(config.getRawSignalCacheSizeInRows())
                .weigher((Weigher<BiosQuery, ISqlResponse>) (query, response) -> getTotalRows(response))
                .expireAfterWrite(config.getRawSignalCacheSeconds(), TimeUnit.SECONDS),
                    new CacheLoader<>() {
                        @Override
                        public ISqlResponse load(final BiosQuery query)
                        {
                            return executeInternal(query);
                        }
                    });

        featureCache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumWeight(config.getFeatureCacheSizeInRows())
                .weigher((Weigher<BiosQuery, ISqlResponse>) (query, response) -> getTotalRows(response))
                .expireAfterWrite(config.getFeatureCacheSeconds(), TimeUnit.SECONDS),
                    new CacheLoader<>() {
                        @Override
                        public ISqlResponse load(final BiosQuery query)
                        {
                            return executeInternal(query);
                        }
                    });

        // Execute one query to initialize bios SDK metrics.
        getQueryResponse(
                new BiosQuery(SCHEMA_SIGNALS, "_requests", System.currentTimeMillis(), -300000L,
                        300L, null, null, new BiosAggregate[]{new BiosAggregate("count", null)}));
    }

    private int getTotalRows(final ISqlResponse response)
    {
        int numRows = 0;
        numRows += response.getRecords().size();
        for (var window : response.getDataWindows()) {
            numRows += window.getRecords().size();
        }
        return numRows;
    }

    /**
     * This method does one of the following:
     * 1. If doNotThrowIfRetryable is true and the exception indicates that the operation is
     * retryable, returns normally after reestablishing a new session.
     * 2. If doNotThrowIfRetryable is false, or if the exception does not look retryable, throws:
     *      a) TrinoException for known/handled exceptions, or
     *      b) RuntimeException for unhandled exceptions.
     */
    private void handleException(Throwable t, boolean doNotThrowIfRetryable)
    {
        if (t instanceof BiosClientException) {
            BiosClientException biosClientException = (BiosClientException) t;
            switch (biosClientException.getCode()) {
                case BAD_INPUT:
                    throw new TrinoException(GENERIC_USER_ERROR, biosClientException.getMessage());

                case SESSION_INACTIVE:
                case CLIENT_CHANNEL_ERROR:
                case SERVER_CONNECTION_FAILURE:
                case SERVER_CHANNEL_ERROR:
                case SERVICE_UNAVAILABLE:
                case SERVICE_UNDEPLOYED:
                case SESSION_EXPIRED:
                    if (doNotThrowIfRetryable) {
                        logger.debug("Possibly retryable error code: %s. \n\n "
                                + "Attempting to create a new session... \n", biosClientException.getCode());
                        session = Suppliers.memoize(sessionSupplier(biosConfig));
                        session.get();
                        return;
                    }
                    // Fallthrough to default.

                default:
                    logger.debug("bi(OS) got exception: %s", t);
                    throw new RuntimeException(biosClientException);
            }
        }
        else {
            logger.debug("bi(OS) got exception: %s", t);
            throw new RuntimeException(t);
        }
    }

    private static Supplier<Session> sessionSupplier(BiosConfig biosConfig)
    {
        return () -> {
            Session session;
            try {
                logger.debug("sessionSupplier: %s (%s), %s", biosConfig.getUrl().toString(),
                        biosConfig.getUrl().getHost(),
                        biosConfig.getEmail());
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
                throw new RuntimeException(e);
            }
        };
    }

    private static Supplier<TenantConfig> tenantConfigSupplier(final BiosClient client)
    {
        return () -> {
            TenantConfig tenantConfig;
            try {
                logger.debug("----------> bios network request : getTenant");
                tenantConfig = client.getSession().getTenant(true, true);
            }
            catch (BiosClientException e) {
                client.handleException(e, true);
                // If handleException did not throw an exception, it means we can retry.
                try {
                    logger.debug("retrying ----------> bios network request : getTenant");
                    tenantConfig = client.getSession().getTenant(true, true);
                }
                catch (BiosClientException e2) {
                    client.handleException(e2, false);  // This should always throw.
                    throw new RuntimeException("This should never happen - gap in bios connector");
                }
            }

            logger.debug("<---------- bios network response: getTenant %s returned %d signals, %d "
                            + "contexts",
                    tenantConfig.getName(), tenantConfig.getSignals().size(),
                    tenantConfig.getContexts().size());
            return tenantConfig;
        };
    }

    public static Long floor(Long toBeFloored, long divisor)
    {
        if (toBeFloored == null) {
            return null;
        }
        return divisor * (toBeFloored / divisor);
    }

    public static Long ceiling(Long toBeCeiled, long divisor)
    {
        if (toBeCeiled == null) {
            return null;
        }
        return (long) Math.signum(toBeCeiled) * divisor * (((Math.abs(toBeCeiled) - 1) / divisor) + 1);
    }

    public long getCurrentTimeWithLag(BiosTableHandle tableHandle)
    {
        long lag = (tableHandle.getTableKind() != BiosTableKind.RAW_SIGNAL) ?
                biosConfig.getFeatureLagSeconds() * 1000 :
                biosConfig.getRawSignalLagSeconds() * 1000;
        return System.currentTimeMillis() - lag;
    }

    public long getEffectiveTimeRangeStart(BiosTableHandle tableHandle)
    {
        long currentTimeWithLag = getCurrentTimeWithLag(tableHandle);
        long start;

        if (tableHandle.getTimeRangeStart() != null) {
            start = tableHandle.getTimeRangeStart();
        }
        else {
            if (tableHandle.getQueryPeriodOffsetSeconds() != null) {
                start = currentTimeWithLag - tableHandle.getQueryPeriodOffsetSeconds() * 1000;
            }
            else {
                start = currentTimeWithLag;
            }
        }

        // If this is a feature, "snap" it to the window size.
        if (tableHandle.getTableKind() != BiosTableKind.RAW_SIGNAL) {
            start = floor(start, getEffectiveWindowSizeSeconds(tableHandle) * 1000);
        }

        // Return the lower bound of the time range.
        long delta = getEffectiveTimeRangeDeltaSigned(tableHandle);
        if (delta >= 0) {
            return start;
        }
        else {
            return start + delta;
        }
    }

    public long getEffectiveTimeRangeDelta(BiosTableHandle tableHandle)
    {
        long delta = getEffectiveTimeRangeDeltaSigned(tableHandle);
        return Math.abs(delta);
    }

    private long getEffectiveTimeRangeDeltaSigned(BiosTableHandle tableHandle)
    {
        long delta;

        if (tableHandle.getTimeRangeDelta() != null) {
            delta = tableHandle.getTimeRangeDelta();
        }
        else {
            if (tableHandle.getQueryPeriodSeconds() != null) {
                delta = -1000 * tableHandle.getQueryPeriodSeconds();
            }
            else {
                delta = -1000 * biosConfig.getDefaultTimeRangeDeltaSeconds();
            }
        }
        return delta;
    }

    // Get window size - relevant for features; use placeholder 1 for raw signals / contexts.
    public long getEffectiveWindowSizeSeconds(BiosTableHandle tableHandle)
    {
        long out;
        if (tableHandle.getWindowSizeSeconds() != null) {
            out = tableHandle.getWindowSizeSeconds();
        }
        else {
            if (tableHandle.getTableKind() == BiosTableKind.SIGNAL) {
                out = biosConfig.getDefaultWindowSizeSeconds();
            }
            else {
                out = 1;
            }
        }
        return out;
    }

    public ISqlResponse getQueryResponse(BiosQuery query)
    {
        switch (query.getTableKind()) {
            case RAW_SIGNAL:
                // For raw signals, get all attributes so that we don't have many queries with
                //      different subsets of attributes.
                query.setAttributes(null);
                break;

            case SIGNAL:
                // Ensure main signals are only used for aggregated results, not raw rows.
                if (query.getAggregates() == null) {
                    throw new TrinoException(GENERIC_USER_ERROR, "Query has no aggregate or has "
                            + "unsupported complex transformation; use raw signals for such "
                            + "queries. Query: " + query);
                }
                break;
        }

        logger.debug("Request : query");
        NonEvictableLoadingCache<BiosQuery, ISqlResponse> cache;
        switch (query.getTableKind()) {
            case CONTEXT:
                cache = contextCache;
                break;
            case RAW_SIGNAL:
                cache = rawSignalCache;
                break;
            case SIGNAL:
                cache = featureCache;
                break;
            default:
                return null;
        }
        var response = cache.getUnchecked(query);
        logger.debug("Response: %d total rows in %d windows", getTotalRows(response),
                response.getDataWindows().size());

        return response;
    }

    private static Metric.MetricFinalSpecifier[] getAggregateMetrics(BiosAggregate[] aggregates)
    {
        List<Metric.MetricFinalSpecifier> metrics = new ArrayList<>();
        for (var aggregate : aggregates) {
            switch (aggregate.getAggregateFunction().toLowerCase(Locale.getDefault())) {
                case "sum":
                    metrics.add(sum(aggregate.getAggregateSource()));
                    break;
                case "min":
                    metrics.add(min(aggregate.getAggregateSource()));
                    break;
                case "max":
                    metrics.add(max(aggregate.getAggregateSource()));
                    break;
                case "count":
                    metrics.add(count());
                    break;
                default:
                    throw new TrinoException(GENERIC_USER_ERROR, "Aggregate " + aggregate.getAggregateFunction() +
                            " is not yet supported on this table; use raw signal instead.");
            }
        }

        return metrics.toArray(Metric.MetricFinalSpecifier[]::new);
    }

    private ISqlResponse executeInternal(BiosQuery query)
    {
        ISqlStatement isqlStatement;

        switch (query.getTableKind()) {
            case CONTEXT:
                // Contexts only support listing the primary key attribute directly.
                // First get all the primary key values, and then issue a second query to get
                // all the attributes for each of those keys.

                var columns = getColumnHandles(query.getSchemaName(), query.getTableName());
                String keyColumnName = columns.get(0).getColumnName();

                ISqlStatement preliminaryStatement = ISqlStatement.select(keyColumnName)
                        .fromContext(query.getUnderlyingTableName())
                        .build();

                ISqlResponse preliminaryResponse = executeStatement(preliminaryStatement);
                String[] keyValues = preliminaryResponse.getRecords().stream()
                        .map(r -> r.getAttribute(keyColumnName).asString())
                        .toArray(String[]::new);

                isqlStatement = ISqlStatement.select()
                        .fromContext(query.getUnderlyingTableName())
                        .where(keys().in((java.lang.Object) keyValues))
                        .build();
                break;

            case SIGNAL:
                var partialStatement =
                        ISqlStatement.select(getAggregateMetrics(query.getAggregates()))
                                .from(query.getUnderlyingTableName());
                if (query.getGroupBy() != null) {
                    partialStatement = partialStatement.groupBy(query.getGroupBy());
                }
                isqlStatement = partialStatement
                        .window(tumbling(query.getWindowSizeSeconds(), TimeUnit.SECONDS))
                        .snappedTimeRange(query.getTimeRangeStart(), query.getTimeRangeDelta())
                        .build();
                break;

            case RAW_SIGNAL:
                isqlStatement = ISqlStatement.select()
                        .from(query.getUnderlyingTableName())
                        .timeRange(query.getTimeRangeStart(), query.getTimeRangeDelta())
                        .build();
                break;

            default:
                return null;
        }

        logger.debug("----------> bios network request : %s", query);
        ISqlResponse response = executeStatement(isqlStatement);
        logger.debug("<---------- bios network response: query returned %d records, %d windows, "
                        + "%d total rows",
                response.getRecords().size(), response.getDataWindows().size(), getTotalRows(response));

        return response;
    }

    private ISqlResponse executeStatement(ISqlStatement statement)
    {
        ISqlResponse response;
        try {
            response = session.get().execute(statement);
        }
        catch (BiosClientException e) {
            handleException(e, true);
            // If handleException did not throw an exception, it means we can retry.
            try {
                logger.debug("retrying ----------> bios network request : query");
                response = session.get().execute(statement);
            }
            catch (BiosClientException e2) {
                handleException(e2, false);
                throw new RuntimeException("This should never happen - gap in bios connector");
            }
        }
        return response;
    }

    public List<String> getSchemaNames()
    {
        return ImmutableList.of(SCHEMA_CONTEXTS, SCHEMA_SIGNALS, SCHEMA_RAW_SIGNALS);
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
            case SCHEMA_SIGNALS:
                for (SignalConfig signalConfig : tenantConfig.get().getSignals()) {
                    tableNames.add(signalConfig.getName());
                }
                break;
            case SCHEMA_RAW_SIGNALS:
                for (SignalConfig signalConfig : tenantConfig.get().getSignals()) {
                    tableNames.add(addRawSuffix(signalConfig.getName()));
                }
                break;
            case SCHEMA_CONTEXTS:
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
        // Check whether this table exists.
        List<AttributeConfig> attributes = getAttributeConfigs(schemaName, tableName);
        if (attributes == null) {
            return null;
        }
        return new BiosTableHandle(schemaName, tableName);
    }

    public List<BiosColumnHandle> getColumnHandles(String schemaName, String tableName)
    {
        // logger.debug("getColumnHandles: %s.%s", schemaName, tableName);
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");

        List<AttributeConfig> attributes = getAttributeConfigs(schemaName, tableName);
        if (attributes == null) {
            return null;
        }

        var tableHandle = new BiosTableHandle(schemaName, tableName);
        BiosTableKind kind = tableHandle.getTableKind();
        ImmutableList.Builder<BiosColumnHandle> columns = ImmutableList.builder();

        String defaultValue;
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
                    defaultValue, (kind == BiosTableKind.CONTEXT) && isFirstAttribute, null, null);
            isFirstAttribute = false;
            columns.add(columnHandle);
        }

        switch (schemaName) {
            case SCHEMA_SIGNALS:
                columns.add(new BiosColumnHandle(COLUMN_WINDOW_BEGIN_TIMESTAMP, TIMESTAMP_SECONDS, null, false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_WINDOW_BEGIN_EPOCH, BIGINT, null, false, null, null));

                columns.add(new BiosColumnHandle(COLUMN_PARAM_QUERY_PERIOD_SECONDS, BIGINT, null, false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_PARAM_QUERY_PERIOD_OFFSET_SECONDS, BIGINT, null, false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_PARAM_WINDOW_SIZE_SECONDS, BIGINT, null, false, null, null));
                break;

            case SCHEMA_RAW_SIGNALS:
                columns.add(new BiosColumnHandle(COLUMN_SIGNAL_TIMESTAMP, TIMESTAMP_MICROS, null, false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_SIGNAL_TIME_EPOCH_MS, BIGINT, null, false, null, null));

                columns.add(new BiosColumnHandle(COLUMN_PARAM_QUERY_PERIOD_SECONDS, BIGINT, null, false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_PARAM_QUERY_PERIOD_OFFSET_SECONDS, BIGINT, null, false, null, null));
                break;

            case SCHEMA_CONTEXTS:
                columns.add(new BiosColumnHandle(COLUMN_CONTEXT_TIMESTAMP, TIMESTAMP_MICROS, null, false, null, null));
                columns.add(new BiosColumnHandle(COLUMN_CONTEXT_TIME_EPOCH_MS, BIGINT, null, false, null, null));
                break;
        }

        return columns.build();
    }

    private List<AttributeConfig> getAttributeConfigs(final String schemaName, final String tableName)
    {
        List<AttributeConfig> attributes = null;

        switch (schemaName) {
            case SCHEMA_SIGNALS:
            case SCHEMA_RAW_SIGNALS:
                final String underlyingTableName;
                if (schemaName.equals(SCHEMA_SIGNALS)) {
                    underlyingTableName = tableName;
                }
                else {
                    underlyingTableName = removeRawSuffix(tableName);
                }
                for (SignalConfig signalConfig : tenantConfig.get().getSignals()) {
                    if (!underlyingTableName.equalsIgnoreCase(signalConfig.getName())) {
                        continue;
                    }
                    attributes = signalConfig.getAttributes();
                    break;
                }
                break;
            case SCHEMA_CONTEXTS:
                for (ContextConfig contextConfig : tenantConfig.get().getContexts()) {
                    if (!tableName.equalsIgnoreCase(contextConfig.getName())) {
                        continue;
                    }
                    attributes = contextConfig.getAttributes();
                    break;
                }
                break;
            default:
                throw new RuntimeException("This should never happen - gap in bios connector");
        }
        return attributes;
    }

    public boolean isSupportedAggregate(String aggregate)
    {
        return aggregates.contains(aggregate.toLowerCase(Locale.getDefault()));
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

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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.AggregationApplicationResult;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.SortItem;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.connector.TopNApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.bios.BiosClient.COLUMN_PARAM_QUERY_PERIOD_MINUTES;
import static io.trino.plugin.bios.BiosClient.COLUMN_PARAM_QUERY_PERIOD_OFFSET_MINUTES;
import static io.trino.plugin.bios.BiosClient.COLUMN_PARAM_WINDOW_SIZE_SECONDS;
import static io.trino.plugin.bios.BiosClient.COLUMN_SIGNAL_TIMESTAMP;
import static io.trino.plugin.bios.BiosClient.COLUMN_SIGNAL_TIME_EPOCH_MS;
import static io.trino.plugin.bios.BiosClient.COLUMN_WINDOW_BEGIN_EPOCH;
import static io.trino.plugin.bios.BiosClient.COLUMN_WINDOW_BEGIN_TIMESTAMP;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class BiosMetadata
        implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(BiosMetadata.class);

    private final BiosClient biosClient;
    private final ConnectorTableProperties connectorTableProperties;

    @Inject
    public BiosMetadata(BiosClient biosClient)
    {
        this.biosClient = requireNonNull(biosClient, "biosClient is null");
        connectorTableProperties = new ConnectorTableProperties();
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(biosClient.getSchemaNames());
    }

    @Override
    public BiosTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        // logger.debug("getTableHandle");
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            logger.debug("getTableHandle - returning null because schema was not found.");
            return null;
        }

        return biosClient.getTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        // logger.debug("getTableMetadata");
        return getTableMetadata(session, ((BiosTableHandle) table).toSchemaTableName());
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session,
                                                    SchemaTableName schemaTableName)
    {
        // logger.debug("getTableMetadata");
        if (!listSchemaNames(session).contains(schemaTableName.getSchemaName())) {
            logger.debug("getTableMetadata called for non-existent schema; returning null.");
            return null;
        }

        var columnHandles = biosClient.getColumnHandles(schemaTableName.getSchemaName(),
                schemaTableName.getTableName());
        if (columnHandles == null) {
            return null;
        }

        List<ColumnMetadata> columns = columnHandles.stream()
                .map(BiosColumnHandle::getColumnMetadata)
                .collect(Collectors.toUnmodifiableList());

        return new ConnectorTableMetadata(schemaTableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        logger.debug("listTables");
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(biosClient.getSchemaNames()));

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : biosClient.getTableNames(schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        // logger.debug("getColumnHandles");
        BiosTableHandle biosTableHandle = (BiosTableHandle) tableHandle;

        var columns = biosClient.getColumnHandles(biosTableHandle.getSchemaName(),
                biosTableHandle.getTableName());
        if (columns == null) {
            throw new TableNotFoundException(biosTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (BiosColumnHandle column : columns) {
            columnHandles.put(column.getColumnName(), column);
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        logger.debug("listTableColumns");
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName schemaTableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, schemaTableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(schemaTableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        // logger.debug("getColumnMetadata");
        return ((BiosColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        // logger.debug("getTableProperties");
        return connectorTableProperties;
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        BiosTableHandle tableHandle = (BiosTableHandle) handle;
        boolean somePushdownApplied = false;
        Map<ColumnHandle, Domain> remainingDomains = new HashMap<>();
        Long timeRangeStart = null;
        Long timeRangeDelta = null;
        Long windowSizeSeconds = null;
        Long queryPeriodMinutes = null;
        Long queryPeriodOffsetMinutes = null;

        if (constraint.getSummary().getDomains().isEmpty()) {
            return Optional.empty();
        }
        for (var entry : constraint.getSummary().getDomains().get().entrySet()) {
            String columnName = ((BiosColumnHandle) entry.getKey()).getColumnName();

            switch (columnName) {
                case COLUMN_SIGNAL_TIMESTAMP:
                case COLUMN_SIGNAL_TIME_EPOCH_MS:
                case COLUMN_WINDOW_BEGIN_EPOCH:
                case COLUMN_WINDOW_BEGIN_TIMESTAMP:
                    // If we have not already set the time range, pushdown time range.
                    if ((timeRangeStart == null)) {
                        long timeRangeLow;
                        long timeRangeHigh;
                        double scalingFactor = 1;   // To convert to milliseconds.
                        switch (columnName) {
                            case COLUMN_SIGNAL_TIMESTAMP:
                            case COLUMN_WINDOW_BEGIN_TIMESTAMP:
                                // Trino uses microseconds since epoch for timestamps, whereas
                                // bios v1 uses milliseconds.
                                scalingFactor = 1.0 / 1000;
                                break;
                            case COLUMN_WINDOW_BEGIN_EPOCH:
                                scalingFactor = 1000;
                                break;
                        }

                        // Currently, we only support pushdown of single range predicates on signal timestamp.
                        var valueSet = entry.getValue().getValues();
                        if (!(valueSet instanceof SortedRangeSet)) {
                            continue;
                        }
                        var sortedRangeSet = (SortedRangeSet) valueSet;
                        if ((sortedRangeSet.getRangeCount() != 1) || sortedRangeSet.isAll() || sortedRangeSet.isNone() || sortedRangeSet.isDiscreteSet()) {
                            continue;
                        }
                        var range = sortedRangeSet.getOrderedRanges().get(0);
                        if (range.getLowValue().isPresent()) {
                            timeRangeLow =
                                    (long) ((Long) range.getLowValue().get() * scalingFactor);
                            // logger.debug("timeRangeLow provided: %d", timeRangeLow);
                        }
                        else {
                            timeRangeLow = 0L;
                        }
                        if (range.getHighValue().isPresent()) {
                            timeRangeHigh =
                                    (long) ((Long) range.getHighValue().get() * scalingFactor);
                            // logger.debug("timeRangeHigh provided: %d", timeRangeHigh);
                        }
                        else {
                            timeRangeHigh = System.currentTimeMillis();
                        }
                        timeRangeStart = timeRangeLow;
                        timeRangeDelta = timeRangeHigh - timeRangeLow;
                        somePushdownApplied = true;
                        logger.debug("pushdown filter: timeRangeStart %d, timeRangeDelta %d",
                                timeRangeStart, timeRangeDelta);
                    }
                    break;

                case COLUMN_PARAM_WINDOW_SIZE_SECONDS:
                    windowSizeSeconds = getLongValuePredicate(entry.getValue(),
                            COLUMN_PARAM_WINDOW_SIZE_SECONDS) * 1000;
                    somePushdownApplied = true;
                    logger.debug("pushdown filter: windowSizeSeconds %d", windowSizeSeconds);
                    break;

                case COLUMN_PARAM_QUERY_PERIOD_MINUTES:
                    queryPeriodMinutes = getLongValuePredicate(entry.getValue(),
                            COLUMN_PARAM_QUERY_PERIOD_MINUTES);
                    somePushdownApplied = true;
                    logger.debug("pushdown filter: queryPeriodMinutes %d", queryPeriodMinutes);
                    break;

                case COLUMN_PARAM_QUERY_PERIOD_OFFSET_MINUTES:
                    queryPeriodOffsetMinutes = getLongValuePredicate(entry.getValue(),
                            COLUMN_PARAM_QUERY_PERIOD_OFFSET_MINUTES);
                    somePushdownApplied = true;
                    logger.debug("pushdown filter: queryPeriodOffsetMinutes %d", queryPeriodOffsetMinutes);
                    break;

                default:
                    remainingDomains.put(entry.getKey(), entry.getValue());
                    logger.debug("Not pushing down: %s", columnName);
                    break;
            }
        }

        if (somePushdownApplied) {
            // logger.debug("applyFilter %s:  constraint: %s  PredicateColumns: %s",
            //         tableHandle.toSchemaTableName(),
            //         constraint.getSummary().toString(),
            //         Arrays.toString(constraint.getPredicateColumns().stream().toArray()));

            TupleDomain<ColumnHandle> remainingFilter = withColumnDomains(remainingDomains);
            return Optional.of(
                    new ConstraintApplicationResult<>(
                            new BiosTableHandle(tableHandle.getSchemaName(), tableHandle.getTableName(),
                                    timeRangeStart, timeRangeDelta, windowSizeSeconds,
                                    queryPeriodMinutes, queryPeriodOffsetMinutes,
                                    tableHandle.getGroupBy()),
                            remainingFilter,
                            false));
        }
        else {
            return Optional.empty();
        }
    }

    private long getLongValuePredicate(final Domain domain, final String columnName)
    {
        if (!domain.isSingleValue()) {
            logger.debug("columnName %s domain %s", columnName, domain);
            throw new TrinoException(GENERIC_USER_ERROR, columnName +
                    " can only be equated to a single value.");
        }
        return (Long) domain.getSingleValue();
    }

    @Override
    public Optional<AggregationApplicationResult<ConnectorTableHandle>> applyAggregation(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<AggregateFunction> aggregates,
            Map<String, ColumnHandle> assignments,
            List<List<ColumnHandle>> groupingSets)
    {
        BiosTableHandle tableHandle = (BiosTableHandle) handle;
        if (tableHandle.getTableKind() != BiosTableKind.SIGNAL) {
            return Optional.empty();
        }

        // Group by has already been applied once, cannot aggregate again.
        if (tableHandle.getGroupBy() != null) {
            return Optional.empty();
        }

        // Not sure how to handle multiple grouping sets.
        if (groupingSets.size() >= 2) {
            return Optional.empty();
        }

        // logger.debug("applyAggregation %s: aggregates: %s  assignments: %s  groupingSets:%s",
        //         tableHandle.toSchemaTableName(), aggregates, assignments, groupingSets);

        List<ConnectorExpression> outProjections = new ArrayList<>();
        List<Assignment> outAssignments = new ArrayList<>();
        List<String> internalAggregateNames = new ArrayList<>();
        for (var aggregate : aggregates) {
            if (!biosClient.isSupportedAggregate(aggregate.getFunctionName())) {
                return Optional.empty();
            }
            Variable input;
            String name;
            if (aggregate.getFunctionName().equalsIgnoreCase("count")) {
                input = new Variable("dummy", BIGINT);
                name = "count()";
            }
            else {
                if (aggregate.getInputs().size() != 1) {
                    throw new TrinoException(GENERIC_USER_ERROR, aggregate.getFunctionName() +
                            " requires exactly 1 input, got " +
                            String.valueOf(aggregate.getInputs().size()));
                }
                input = (Variable) aggregate.getInputs().get(0);
                name = aggregate.getFunctionName().toLowerCase(Locale.getDefault()) + "(" +
                        input.getName().toLowerCase(Locale.getDefault()) + ")";
            }
            outProjections.add(new Variable(name, aggregate.getOutputType()));
            outAssignments.add(new Assignment(name,
                    new BiosColumnHandle(name, aggregate.getOutputType(), null, false,
                            aggregate.getFunctionName(), input.getName()),
                    aggregate.getOutputType()));
            internalAggregateNames.add(name);
        }

        String[] groupBy = null;
        if (groupingSets.size() > 0) {
            groupBy = groupingSets.get(0).stream()
                    .filter(ch -> !(((BiosColumnHandle) ch).getIsVirtual()))
                    .map(ch -> ((BiosColumnHandle) ch).getColumnName())
                    .toArray(String[]::new);
        }
        var outTableHandle = new BiosTableHandle(tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                tableHandle.getTimeRangeStart(), tableHandle.timeRangeDelta,
                tableHandle.getWindowSizeSeconds(), tableHandle.getQueryPeriodMinutes(),
                tableHandle.getQueryPeriodOffsetMinutes(), groupBy);

        logger.debug("pushdown aggregates: %s,  groupBy: %s", internalAggregateNames,
                Arrays.toString(groupBy));

        var outResult = new AggregationApplicationResult<ConnectorTableHandle>(outTableHandle,
                outProjections, outAssignments, new HashMap<>(), false);
        return Optional.of(outResult);
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        // BiosTableHandle tableHandle = (BiosTableHandle) handle;
        // logger.debug("applyProjection %s: projections: %s  assignments: %s",
        //         tableHandle, projections, assignments);

        return Optional.empty();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        // BiosTableHandle tableHandle = (BiosTableHandle) handle;
        // logger.debug("applyLimit %s: limit: %d", tableHandle, limit);

        return Optional.empty();
    }

    @Override
    public Optional<TopNApplicationResult<ConnectorTableHandle>> applyTopN(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long topNCount,
            List<SortItem> sortItems,
            Map<String, ColumnHandle> assignments)
    {
        // BiosTableHandle tableHandle = (BiosTableHandle) handle;
        // logger.debug("applyTopN %s: topNCount: %d  sortItems: %s  assignments: %s",
        //         tableHandle, topNCount, sortItems, assignments);

        return Optional.empty();
    }
}

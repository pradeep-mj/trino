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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.bios.BiosClient.SIGNAL_TIMESTAMP_COLUMN;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class BiosMetadata
        implements ConnectorMetadata
{
    private static final Logger logger = Logger.get(BiosMetadata.class);

    private final BiosClient biosClient;

    @Inject
    public BiosMetadata(BiosClient biosClient)
    {
        this.biosClient = requireNonNull(biosClient, "biosClient is null");
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
            return null;
        }

        BiosTable table = biosClient.getTable(tableName.getSchemaName(), tableName.getTableName());
        if (table == null) {
            return null;
        }

        return table.getTableHandle();
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
        if (!listSchemaNames(session).contains(schemaTableName.getSchemaName())) {
            return null;
        }

        BiosTable biosTable = biosClient.getTable(schemaTableName.getSchemaName(),
                schemaTableName.getTableName());
        if (biosTable == null) {
            return null;
        }

        List<ColumnMetadata> columns = biosTable.getColumns().stream()
                .map(BiosColumnHandle::getColumnMetadata)
                .collect(toList());
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

        BiosTable table = biosClient.getTable(biosTableHandle.getSchemaName(), biosTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(biosTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (BiosColumnHandle column : table.getColumns()) {
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
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        BiosTableHandle handle = (BiosTableHandle) tableHandle;
        long timeRangeLow;
        long timeRangeHigh;
        Long timeRangeStart = null;
        Long timeRangeDelta = null;
        Map<ColumnHandle, Domain> remainingDomains = new HashMap<>();

        logger.debug("applyFilter %s: constraint: %s  %s  %s",
                handle.toSchemaTableName(),
                constraint.getSummary().toString(), constraint.predicate().toString(),
                Arrays.toString(constraint.getPredicateColumns().stream().toArray()));

        if (constraint.getSummary().getDomains().isEmpty()) {
            return Optional.empty();
        }
        for (var entry : constraint.getSummary().getDomains().get().entrySet()) {
            // For any column other than signal timestamp, do not pushdown.
            if (!((BiosColumnHandle) entry.getKey()).getColumnName().equals(SIGNAL_TIMESTAMP_COLUMN)) {
                remainingDomains.put(entry.getKey(), entry.getValue());
                logger.debug("Not pushing down: %s", ((BiosColumnHandle) entry.getKey()).getColumnName());
                continue;
            }

            // Currently, we only support pushdown of single range predicates on signal timestamp.
            var valueSet = entry.getValue().getValues();
            if (!(valueSet instanceof SortedRangeSet)) {
                return Optional.empty();
            }
            var sortedRangeSet = (SortedRangeSet) valueSet;
            if ((sortedRangeSet.getRangeCount() != 1) || sortedRangeSet.isAll() || sortedRangeSet.isNone() || sortedRangeSet.isDiscreteSet()) {
                return Optional.empty();
            }
            var range = sortedRangeSet.getOrderedRanges().get(0);
            if (range.getLowValue().isPresent()) {
                timeRangeLow = (Long) range.getLowValue().get() / 1000;
                logger.debug("timeRangeLow provided: %d", timeRangeLow);
            }
            else {
                timeRangeLow = 0L;
            }
            if (range.getHighValue().isPresent()) {
                timeRangeHigh = (Long) range.getHighValue().get() / 1000;
                logger.debug("timeRangeHigh provided: %d", timeRangeHigh);
            }
            else {
                timeRangeHigh = System.currentTimeMillis();
            }
            timeRangeStart = timeRangeLow;
            timeRangeDelta = timeRangeHigh - timeRangeLow;
        }

        if (timeRangeStart != null) {
            logger.debug("pushdown: timeRangeStart %d, timeRangeDelta %d", timeRangeStart, timeRangeDelta);

            TupleDomain<ColumnHandle> remainingFilter = withColumnDomains(remainingDomains);
            return Optional.of(
                    new ConstraintApplicationResult<>(
                            new BiosTableHandle(handle.getSchemaName(), handle.getTableName(),
                                    timeRangeStart, timeRangeDelta),
                            remainingFilter,
                            false));
        }
        else {
            return Optional.empty();
        }
    }
}

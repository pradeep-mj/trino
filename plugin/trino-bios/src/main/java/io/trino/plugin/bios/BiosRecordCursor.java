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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.isima.bios.models.DataWindow;
import io.isima.bios.models.Record;
import io.isima.bios.models.isql.ISqlResponse;
import io.isima.bios.models.isql.ISqlStatement;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.isima.bios.models.isql.WhereClause.keys;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Objects.requireNonNull;

public class BiosRecordCursor
        implements RecordCursor
{
    private static final Logger logger = Logger.get(BiosRecordCursor.class);

    private final BiosClient biosClient;
    private final BiosTableHandle tableHandle;
    private final List<BiosColumnHandle> columnHandles;
    private Iterator<Record> records;
    private Record currentRecord;

    public BiosRecordCursor(BiosClient biosClient, BiosTableHandle tableHandle,
                            List<BiosColumnHandle> columnHandles)
    {
        this.biosClient = requireNonNull(biosClient, "biosClient is null");
        this.tableHandle = requireNonNull(tableHandle, "statement is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (records == null) {
            String[] attributes = columnHandles.stream()
                    .map(BiosColumnHandle::getColumnName)
                    .toArray(String[]::new);
            ISqlStatement statement;

            if (tableHandle.getKind() == BiosTableKind.SIGNAL) {
                // For signals, make a simple time-range query.

                // TODO make start and delta dynamically assignable per query.
                long start = System.currentTimeMillis();
                long delta = -(60 * 60 * 1000);
                statement = ISqlStatement.select(attributes)
                        .from(tableHandle.getTableName())
                        .timeRange(start, delta)
                        .build();
            }
            else {
                // Contexts only support listing the primary key attribute directly.
                // First get all the primary key values, and then issue a second query to get
                // all the attributes for each of those keys.

                BiosTable table = biosClient.getTable(tableHandle.getSchemaName(),
                        tableHandle.getTableName());
                String keyColumnName = table.getColumns().get(0).getColumnName();
                ISqlStatement preliminaryStatement = ISqlStatement.select(keyColumnName)
                        .fromContext(tableHandle.getTableName())
                        .build();
                ISqlResponse preliminaryResponse;
                try {
                    preliminaryResponse = biosClient.getSession().execute(preliminaryStatement);
                }
                catch (BiosClientException e) {
                    throw new RuntimeException(e.toString());
                }
                logger.debug("BiosRecordCursor: preliminaryResponse %d windows, %d records",
                        preliminaryResponse.getDataWindows().size(),
                        preliminaryResponse.getRecords().size());
                String[] keyValues = preliminaryResponse.getRecords().stream()
                        .map(r -> r.getAttribute(keyColumnName).asString())
                        .toArray(String[]::new);

                statement = ISqlStatement.select()
                        .fromContext(tableHandle.getTableName())
                        .where(keys().in(keyValues))
                        .build();
            }

            ISqlResponse response;
            try {
                response = biosClient.getSession().execute(statement);
            }
            catch (BiosClientException e) {
                throw new RuntimeException(e.toString());
            }
            logger.debug("BiosRecordCursor: %d windows, %d records",
                    response.getDataWindows().size(),
                    response.getRecords().size());

            List<Record> data = new ArrayList<>(response.getRecords());
            for (DataWindow window : response.getDataWindows()) {
                logger.debug("BiosRecordCursor: %d records", window.getRecords().size());
                data.addAll(window.getRecords());
            }
            records = data.iterator();
        }
        if (!records.hasNext()) {
            return false;
        }
        currentRecord = records.next();
        return true;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asBoolean();
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asLong();
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asDouble();
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(
                currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asString());
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return false;
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}

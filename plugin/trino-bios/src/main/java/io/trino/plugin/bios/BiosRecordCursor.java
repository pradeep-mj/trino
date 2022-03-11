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
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.bios.BiosClient.CONTEXT_TIMESTAMP_COLUMN;
import static io.trino.plugin.bios.BiosClient.SIGNAL_TIMESTAMP_COLUMN;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
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
            logger.debug("bios got query on table %s", tableHandle.getTableName());
            String[] attributes = columnHandles.stream()
                    .map(BiosColumnHandle::getColumnName)
                    .filter(a -> !Objects.equals(a, SIGNAL_TIMESTAMP_COLUMN))
                    .filter(a -> !Objects.equals(a, CONTEXT_TIMESTAMP_COLUMN))
                    .toArray(String[]::new);
            BiosStatement statement;

            if (tableHandle.getKind() == BiosTableKind.SIGNAL) {
                // For signals, make a simple time-range query.

                long start;
                long delta;
                if (tableHandle.getTimeRangeStart() != null) {
                    start = tableHandle.getTimeRangeStart();
                    delta = tableHandle.getTimeRangeDelta();
                    logger.debug("Using provided start %d, delta %d", start, delta);
                }
                else {
                    start = System.currentTimeMillis();
                    if (biosClient.getBiosConfig().getDefaultTimeRangeDeltaSeconds() != null) {
                        delta = -1000 * biosClient.getBiosConfig().getDefaultTimeRangeDeltaSeconds();
                    }
                    else {
                        // Use 1 hour if no default is configured.
                        delta = -1000 * 60 * 60;
                    }
                }
                statement = new BiosStatement(BiosTableKind.SIGNAL, tableHandle.getTableName(),
                        attributes, null, start, delta);
            }
            else {
                // Contexts only support listing the primary key attribute directly.
                // First get all the primary key values, and then issue a second query to get
                // all the attributes for each of those keys.

                BiosTable table = biosClient.getTable(tableHandle.getSchemaName(),
                        tableHandle.getTableName());
                String keyColumnName = table.getColumns().get(0).getColumnName();
                BiosStatement preliminaryStatement = new BiosStatement(BiosTableKind.CONTEXT,
                        tableHandle.getTableName(), new String[]{keyColumnName}, null, null, null);
                ISqlResponse preliminaryResponse = biosClient.execute(preliminaryStatement);
                String[] keyValues = preliminaryResponse.getRecords().stream()
                        .map(r -> r.getAttribute(keyColumnName).asString())
                        .toArray(String[]::new);

                statement = new BiosStatement(BiosTableKind.CONTEXT, tableHandle.getTableName(),
                        null, keyValues, null, null);
            }

            ISqlResponse response = biosClient.execute(statement);
            List<Record> data = new ArrayList<>(response.getRecords());
            for (DataWindow window : response.getDataWindows()) {
                logger.debug("BiosRecordCursor: %d records", window.getRecords().size());
                data.addAll(window.getRecords());
            }
            records = data.iterator();

            // if (data.size() > 0) {
            //     logger.debug("First record: timestamp: %d", data.get(0).getTimestamp());
            //     logger.debug("First record: %s", data.get(0).toString());
            //     logger.debug("First record: %s", Arrays.toString(data.get(0).attributes().toArray()));
            // }
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
        // If this is the timestamp column, use getTimestamp instead of asking for an attribute.
        if (columnHandles.get(field).getColumnName().equals(SIGNAL_TIMESTAMP_COLUMN) ||
                columnHandles.get(field).getColumnName().equals(CONTEXT_TIMESTAMP_COLUMN)) {
            checkFieldType(field, TIMESTAMP_MICROS);
            // bios v1 uses milliseconds since epoch, but we want to use microseconds since epoch
            // for v2 compatibility; convert to micros.
            return currentRecord.getTimestamp() * 1000;
        }
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
        Type type = getType(field);
        if (type.equals(VARCHAR)) {
            return Slices.utf8Slice(
                    currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asString());
        }
        else if (type.equals(VARBINARY)) {
            // TODO: Enable this after Bios Java SDK supports blob data types
            // return Slices.wrappedBuffer(
            //         currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asByteArray());
            return Slices.EMPTY_SLICE;
        }
        else {
            checkArgument(false, "Expected field %s to be type VARCHAR or VARBINARY, but is %s",
                    field, type);
            return null;
        }
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

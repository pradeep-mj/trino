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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.bios.BiosClient.COLUMN_CONTEXT_TIMESTAMP;
import static io.trino.plugin.bios.BiosClient.COLUMN_CONTEXT_TIME_EPOCH_MS;
import static io.trino.plugin.bios.BiosClient.COLUMN_SIGNAL_TIMESTAMP;
import static io.trino.plugin.bios.BiosClient.COLUMN_SIGNAL_TIME_EPOCH_MS;
import static io.trino.plugin.bios.BiosClient.COLUMN_WINDOW_BEGIN_EPOCH;
import static io.trino.plugin.bios.BiosClient.COLUMN_WINDOW_BEGIN_TIMESTAMP;
import static io.trino.plugin.bios.BiosClient.COLUMN_WINDOW_SIZE_SECONDS;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
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
    private Iterator<DataWindow> windows;
    private Iterator<Record> records;
    private DataWindow currentWindow;
    private Long currentWindowNum;
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

    private boolean isWindowed()
    {
        return tableHandle.getTableKind() != BiosTableKind.CONTEXT;
    }

    @Override
    public boolean advanceNextPosition()
    {
        // Run the query if it has not already been run.
        if ((windows == null) && (records == null)) {
            logger.debug("bios got query on table %s", tableHandle.getTableName());

            String[] attributes = columnHandles.stream()
                    .filter(ch -> !ch.getIsVirtual())
                    .filter(ch -> !ch.getIsAggregate())
                    .map(BiosColumnHandle::getColumnName)
                    .toArray(String[]::new);
            BiosAggregate[] aggregates = columnHandles.stream()
                    .filter(ch -> !ch.getIsVirtual())
                    .filter(BiosColumnHandle::getIsAggregate)
                    .map(ch -> new BiosAggregate(ch.getAggregateFunction(), ch.getAggregateSource()))
                    .toArray(BiosAggregate[]::new);

            BiosQuery query = new BiosQuery(tableHandle.duplicate(), attributes, aggregates);
            ISqlResponse response = biosClient.getQueryResponse(query);

            if (isWindowed()) {
                windows = response.getDataWindows().iterator();
                if (windows.hasNext()) {
                    currentWindow = windows.next();
                    currentWindowNum = 0L;
                    records = currentWindow.getRecords().iterator();
                }
            }
            else {
                List<Record> data = new ArrayList<>(response.getRecords());
                records = data.iterator();
                logger.debug("     %d records", data.size());
            }
        }

        // Get the next record if present.
        if (isWindowed()) {
            if (records == null) {
                // We have no more records left.
                return false;
            }
            if (records.hasNext()) {
                currentRecord = records.next();
                return true;
            }
            else {
                // This window has run out of records; use the next window that has records.
                while (windows.hasNext()) {
                    currentWindow = windows.next();
                    currentWindowNum++;
                    records = currentWindow.getRecords().iterator();
                    if (records.hasNext()) {
                        currentRecord = records.next();
                        return true;
                    }
                }
                // There are no more windows, so no more records left.
                records = null;
                return false;
            }
        }
        else {
            // The non-windowed case is simple - a single list of records.
            if (records.hasNext()) {
                currentRecord = records.next();
                return true;
            }
            else {
                return false;
            }
        }
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
        String columnName = columnHandles.get(field).getColumnName();

        switch (columnName) {
            case COLUMN_SIGNAL_TIMESTAMP:
            case COLUMN_CONTEXT_TIMESTAMP:
                // bios v1 uses milliseconds since epoch, but Trino uses
                // microseconds since epoch for timestamps; convert to micros.
                return currentRecord.getTimestamp() * 1000;

            case COLUMN_SIGNAL_TIME_EPOCH_MS:
            case COLUMN_CONTEXT_TIME_EPOCH_MS:
                checkFieldType(field, BIGINT);
                return currentRecord.getTimestamp();

            case COLUMN_WINDOW_SIZE_SECONDS:
                throw new TrinoException(GENERIC_USER_ERROR, COLUMN_WINDOW_SIZE_SECONDS +
                        " can only be used in the where clause.");

            case COLUMN_WINDOW_BEGIN_EPOCH:
                if (!isWindowed()) {
                    throw new TrinoException(GENERIC_USER_ERROR, COLUMN_WINDOW_BEGIN_EPOCH +
                            " can only be used for a windowed query.");
                }
                // bios v1 uses milliseconds since epoch, but this virtual column is in seconds.
                return currentWindow.getWindowBeginTime() / 1000;

            case COLUMN_WINDOW_BEGIN_TIMESTAMP:
                if (!isWindowed()) {
                    throw new TrinoException(GENERIC_USER_ERROR, COLUMN_WINDOW_BEGIN_TIMESTAMP +
                            " can only be used for a windowed query.");
                }
                // bios v1 uses milliseconds since epoch, but Trino uses
                // microseconds since epoch for timestamps; convert to micros.
                return currentWindow.getWindowBeginTime() * 1000;

            default:
                checkFieldType(field, BIGINT);
                return currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asLong();
        }
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

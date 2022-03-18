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
import java.util.Locale;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.bios.BiosClient.COLUMN_CONTEXT_TIMESTAMP;
import static io.trino.plugin.bios.BiosClient.COLUMN_CONTEXT_TIME_EPOCH_MS;
import static io.trino.plugin.bios.BiosClient.COLUMN_PARAM_QUERY_PERIOD_MINUTES;
import static io.trino.plugin.bios.BiosClient.COLUMN_PARAM_QUERY_PERIOD_OFFSET_MINUTES;
import static io.trino.plugin.bios.BiosClient.COLUMN_PARAM_WINDOW_SIZE_SECONDS;
import static io.trino.plugin.bios.BiosClient.COLUMN_SIGNAL_TIMESTAMP;
import static io.trino.plugin.bios.BiosClient.COLUMN_SIGNAL_TIME_EPOCH_MS;
import static io.trino.plugin.bios.BiosClient.COLUMN_WINDOW_BEGIN_EPOCH;
import static io.trino.plugin.bios.BiosClient.COLUMN_WINDOW_BEGIN_TIMESTAMP;
import static io.trino.plugin.bios.BiosClient.MAX_WINDOW_BEGIN_EPOCH;
import static io.trino.plugin.bios.BiosClient.MAX_WINDOW_BEGIN_TIMESTAMP;
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
    private final BiosSplit biosSplit;
    private final long timeRangeStart;
    private final long timeRangeEnd;

    private Iterator<DataWindow> windows;
    private Iterator<Record> records;
    private DataWindow currentWindow;
    private Record currentRecord;

    public BiosRecordCursor(BiosClient biosClient, BiosTableHandle tableHandle,
                            List<BiosColumnHandle> columnHandles, BiosSplit biosSplit)
    {
        this.biosClient = requireNonNull(biosClient, "biosClient is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.biosSplit = requireNonNull(biosSplit, "biosSplit is null");

        this.timeRangeStart = biosClient.getEffectiveTimeRangeStart(tableHandle);
        this.timeRangeEnd =
                this.timeRangeStart + biosClient.getEffectiveTimeRangeDelta(tableHandle);
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
        return BiosTableKind.getTableKind(tableHandle.getSchemaName()) != BiosTableKind.CONTEXT;
    }

    private boolean isWithinRequestedTimeRange(long recordTime)
    {
        final var matches = (recordTime >= timeRangeStart) && (recordTime < timeRangeEnd);
        logger.debug("timeRangeStart %d, recordTime %d, timeRangeEnd %d, matches %s",
                timeRangeStart, recordTime, timeRangeEnd, matches);
        return matches;
    }

    @Override
    public boolean advanceNextPosition()
    {
        // Run the query if it has not already been run.
        if ((windows == null) && (records == null)) {
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
            if (aggregates.length == 0) {
                aggregates = null;
            }

            BiosQuery query = new BiosQuery(tableHandle.getSchemaName(), tableHandle.getTableName(),
                    biosSplit.getTimeRangeStart(), biosSplit.getTimeRangeDelta(),
                    biosClient.getEffectiveWindowSizeSeconds(tableHandle),
                    tableHandle.getGroupBy(), attributes, aggregates);
            ISqlResponse response = biosClient.getQueryResponse(query);

            if (isWindowed()) {
                windows = response.getDataWindows().iterator();
                moveToNextApplicableWindow();
            }
            else {
                // Non-windowed results are for contexts, which do not have time range constraints.
                List<Record> data = new ArrayList<>(response.getRecords());
                records = data.iterator();
            }
        }

        // Get the next record if present.
        if (isWindowed()) {
            if (records == null) {
                // We have no more records left.
                return false;
            }
            if (!records.hasNext()) {
                // This window has run out of records; use the next window that is within the
                // requested time range and has records.
                if (!moveToNextApplicableWindow()) {
                    // There are no more windows, so no more records left.
                    records = null;
                    return false;
                }
            }
            currentRecord = records.next();
            logger.debug("selected: %d - %s", currentRecord.getTimestamp(),
                    currentRecord.attributes());
            return true;
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

    private boolean moveToNextApplicableWindow()
    {
        while (windows.hasNext()) {
            currentWindow = windows.next();
            if (!isWithinRequestedTimeRange(currentWindow.getWindowBeginTime())) {
                continue;
            }
            records = currentWindow.getRecords().iterator();
            if (records.hasNext()) {
                logger.debug("selected window: %d - %d records",
                        currentWindow.getWindowBeginTime(), currentWindow.getRecords().size());
                return true;
            }
        }
        return false;
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

        switch (columnName.toLowerCase(Locale.getDefault())) {
            case COLUMN_SIGNAL_TIMESTAMP:
            case COLUMN_CONTEXT_TIMESTAMP:
                // bios v1 uses milliseconds since epoch, but Trino uses
                // microseconds since epoch for timestamps; convert to micros.
                return currentRecord.getTimestamp() * 1000;

            case COLUMN_SIGNAL_TIME_EPOCH_MS:
            case COLUMN_CONTEXT_TIME_EPOCH_MS:
                checkFieldType(field, BIGINT);
                return currentRecord.getTimestamp();

            case COLUMN_WINDOW_BEGIN_EPOCH:
            case MAX_WINDOW_BEGIN_EPOCH:
                if (!isWindowed()) {
                    throw new TrinoException(GENERIC_USER_ERROR, COLUMN_WINDOW_BEGIN_EPOCH +
                            " can only be used for a windowed query.");
                }
                // bios v1 uses milliseconds since epoch, but this virtual column is in seconds.
                return currentWindow.getWindowBeginTime() / 1000;

            case COLUMN_WINDOW_BEGIN_TIMESTAMP:
            case MAX_WINDOW_BEGIN_TIMESTAMP:
                if (!isWindowed()) {
                    throw new TrinoException(GENERIC_USER_ERROR, COLUMN_WINDOW_BEGIN_TIMESTAMP +
                            " can only be used for a windowed query.");
                }
                // bios v1 uses milliseconds since epoch, but Trino uses
                // microseconds since epoch for timestamps; convert to micros.
                return currentWindow.getWindowBeginTime() * 1000;

            case COLUMN_PARAM_WINDOW_SIZE_SECONDS:
            case COLUMN_PARAM_QUERY_PERIOD_MINUTES:
            case COLUMN_PARAM_QUERY_PERIOD_OFFSET_MINUTES:
                throw new TrinoException(GENERIC_USER_ERROR, columnName +
                        " can only be used in the where clause.");

            default:
                checkFieldType(field, BIGINT);
                logger.debug("returning: %d - %s - %d", currentRecord.getTimestamp(),
                        currentRecord.attributes(), currentRecord.getAttribute(columnHandles.get(field).getColumnName()).asLong());
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
            return Slices.EMPTY_SLICE;
        }
        else {
            throw new IllegalArgumentException(String.format(
                    "Expected field %s (%s) to be type VARCHAR or VARBINARY, but is %s", field,
                    columnHandles.get(field).getColumnName(), type));
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
        checkArgument(actual.equals(expected), "Expected field %s (%s) to be type %s but is %s",
                field, columnHandles.get(field).getColumnName(), expected, actual);
    }

    @Override
    public void close()
    {
    }
}

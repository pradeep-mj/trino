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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class BiosTableHandle
        implements ConnectorTableHandle
{
    protected final String schemaName;
    protected final String tableName;
    protected Long timeRangeStart;
    protected Long timeRangeDelta;
    protected Long windowSize;
    protected String[] groupBy;

    @JsonCreator
    public BiosTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("timeRangeStart") Long timeRangeStart,
            @JsonProperty("timeRangeDelta") Long timeRangeDelta,
            @JsonProperty("windowSize") Long windowSize,
            @JsonProperty("groupBy") String[] groupBy)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.timeRangeStart = timeRangeStart;
        this.timeRangeDelta = timeRangeDelta;
        this.windowSize = windowSize;
        this.groupBy = groupBy;
    }

    public BiosTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Long getTimeRangeStart()
    {
        return timeRangeStart;
    }

    public void setTimeRangeStart(Long timeRangeStart)
    {
        this.timeRangeStart = timeRangeStart;
    }

    @JsonProperty
    public Long getTimeRangeDelta()
    {
        return timeRangeDelta;
    }

    public void setTimeRangeDelta(Long timeRangeDelta)
    {
        this.timeRangeDelta = timeRangeDelta;
    }

    @JsonProperty
    public Long getWindowSize()
    {
        return windowSize;
    }

    public void setWindowSize(Long windowSize)
    {
        this.windowSize = windowSize;
    }

    @JsonProperty
    public String[] getGroupBy()
    {
        return groupBy;
    }

    public BiosTableKind getTableKind()
    {
        switch (schemaName) {
            case "context":
                return BiosTableKind.CONTEXT;
            case "signal":
                return BiosTableKind.SIGNAL;
            case "raw_signal":
                return BiosTableKind.RAW_SIGNAL;
            default:
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        "bi(OS) was given invalid schema name: " + schemaName);
        }
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, timeRangeStart, timeRangeDelta, windowSize);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        BiosTableHandle other = (BiosTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.timeRangeStart, other.timeRangeStart) &&
                Objects.equals(this.timeRangeDelta, other.timeRangeDelta) &&
                Objects.equals(this.windowSize, other.windowSize);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("timeRangeStart", timeRangeStart)
                .add("timeRangeDelta", timeRangeDelta)
                .add("windowSize", windowSize)
                .omitNullValues()
                .toString();
    }
}

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

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.bios.BiosClient.SCHEMA_CONTEXTS;
import static io.trino.plugin.bios.BiosClient.SCHEMA_RAW_SIGNALS;
import static io.trino.plugin.bios.BiosClient.SCHEMA_SIGNALS;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class BiosTableHandle
        implements ConnectorTableHandle
{
    protected final String schemaName;
    protected final String tableName;
    protected Long timeRangeStart;
    protected Long timeRangeDelta;
    protected Long windowSizeSeconds;
    protected Long queryPeriodMinutes;
    protected Long queryPeriodOffsetMinutes;
    protected String[] groupBy;

    @JsonCreator
    public BiosTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("timeRangeStart") Long timeRangeStart,
            @JsonProperty("timeRangeDelta") Long timeRangeDelta,
            @JsonProperty("windowSizeSeconds") Long windowSizeSeconds,
            @JsonProperty("queryPeriodMinutes") Long queryPeriodMinutes,
            @JsonProperty("queryPeriodOffsetMinutes") Long queryPeriodOffsetMinutes,
            @JsonProperty("groupBy") String[] groupBy)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.timeRangeStart = timeRangeStart;
        this.timeRangeDelta = timeRangeDelta;
        this.windowSizeSeconds = windowSizeSeconds;
        this.queryPeriodMinutes = queryPeriodMinutes;
        this.queryPeriodOffsetMinutes = queryPeriodOffsetMinutes;
        this.groupBy = groupBy;
    }

    public BiosTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public BiosTableHandle duplicate()
    {
        return new BiosTableHandle(schemaName, tableName, timeRangeStart, timeRangeDelta,
                windowSizeSeconds, queryPeriodMinutes, queryPeriodOffsetMinutes, groupBy);
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

    public String getUnderlyingTableName()
    {
        if (getTableKind() == BiosTableKind.RAW_SIGNAL) {
            return BiosClient.removeRawSuffix(getTableName());
        }
        else {
            return getTableName();
        }
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
    public Long getWindowSizeSeconds()
    {
        return windowSizeSeconds;
    }

    public void setWindowSizeSeconds(Long windowSizeSeconds)
    {
        this.windowSizeSeconds = windowSizeSeconds;
    }

    @JsonProperty
    public Long getQueryPeriodMinutes()
    {
        return queryPeriodMinutes;
    }

    @JsonProperty
    public Long getQueryPeriodOffsetMinutes()
    {
        return queryPeriodOffsetMinutes;
    }

    @JsonProperty
    public String[] getGroupBy()
    {
        return groupBy;
    }

    public BiosTableKind getTableKind()
    {
        switch (schemaName) {
            case SCHEMA_CONTEXTS:
                return BiosTableKind.CONTEXT;
            case SCHEMA_SIGNALS:
                return BiosTableKind.SIGNAL;
            case SCHEMA_RAW_SIGNALS:
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
        return Objects.hash(schemaName, tableName, timeRangeStart, timeRangeDelta,
                windowSizeSeconds, queryPeriodMinutes, queryPeriodOffsetMinutes,
                Arrays.hashCode(groupBy));
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
                Objects.equals(this.windowSizeSeconds, other.windowSizeSeconds) &&
                Objects.equals(this.queryPeriodMinutes, other.queryPeriodMinutes) &&
                Objects.equals(this.queryPeriodOffsetMinutes, other.queryPeriodOffsetMinutes) &&
                Arrays.equals(this.groupBy, other.groupBy);
    }

    @Override
    public String toString()
    {
        return toStringHelper("table")
                .add("", toSchemaTableName())
                .add("start", timeRangeStart)
                .add("delta", timeRangeDelta)
                .add("window", windowSizeSeconds)
                .add("period", queryPeriodMinutes)
                .add("offset", queryPeriodOffsetMinutes)
                .add("groupBy", groupBy)
                .omitNullValues()
                .toString();
    }
}

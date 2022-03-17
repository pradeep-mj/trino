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
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class BiosTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private Long timeRangeStart;
    private Long timeRangeDelta;
    private Long windowSizeSeconds;
    private Long queryPeriodMinutes;
    private Long queryPeriodOffsetMinutes;
    private String[] groupBy;

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

    @JsonProperty
    public Long getTimeRangeDelta()
    {
        return timeRangeDelta;
    }

    @JsonProperty
    public Long getWindowSizeSeconds()
    {
        return windowSizeSeconds;
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

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public BiosTableKind getTableKind()
    {
        return BiosTableKind.getTableKind(schemaName);
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

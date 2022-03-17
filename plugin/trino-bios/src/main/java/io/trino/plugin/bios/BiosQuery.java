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
import io.trino.spi.connector.SchemaTableName;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class BiosQuery
{
    private final String schemaName;
    private final String tableName;
    private final Long timeRangeStart;
    private final Long timeRangeDelta;
    private final Long windowSizeSeconds;
    private final String[] groupBy;
    private String[] attributes;
    private final BiosAggregate[] aggregates;

    @JsonCreator
    public BiosQuery(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("timeRangeStart") Long timeRangeStart,
            @JsonProperty("timeRangeDelta") Long timeRangeDelta,
            @JsonProperty("windowSizeSeconds") Long windowSizeSeconds,
            @JsonProperty("groupBy") String[] groupBy,
            @JsonProperty("attributes") String[] attributes,
            @JsonProperty("aggregates") BiosAggregate[] aggregates)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.timeRangeStart = timeRangeStart;
        this.timeRangeDelta = timeRangeDelta;
        this.windowSizeSeconds = windowSizeSeconds;
        this.groupBy = nullIfEmpty(groupBy);
        this.attributes = nullIfEmpty(attributes);
        this.aggregates = nullIfEmpty(aggregates);
    }

    private <T> T[] nullIfEmpty(T[] in)
    {
        if (in == null) {
            return null;
        }
        if (in.length == 0) {
            return null;
        }
        return in;
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
    public String[] getGroupBy()
    {
        return groupBy;
    }

    @JsonProperty
    public String[] getAttributes()
    {
        return attributes;
    }

    public void setAttributes(String[] attributes)
    {
        this.attributes = attributes;
    }

    @JsonProperty
    public BiosAggregate[] getAggregates()
    {
        return aggregates;
    }

    public BiosTableKind getTableKind()
    {
        return BiosTableKind.getTableKind(schemaName);
    }

    public String getUnderlyingTableName()
    {
        if (BiosTableKind.getTableKind(schemaName) == BiosTableKind.RAW_SIGNAL) {
            return BiosClient.removeRawSuffix(getTableName());
        }
        else {
            return getTableName();
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, timeRangeStart, timeRangeDelta,
                windowSizeSeconds, Arrays.hashCode(groupBy),
                Arrays.hashCode(attributes), Arrays.hashCode(aggregates));
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

        BiosQuery other = (BiosQuery) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.timeRangeStart, other.timeRangeStart) &&
                Objects.equals(this.timeRangeDelta, other.timeRangeDelta) &&
                Objects.equals(this.windowSizeSeconds, other.windowSizeSeconds) &&
                Arrays.equals(this.groupBy, other.groupBy) &&
                Arrays.equals(this.attributes, other.attributes) &&
                Arrays.equals(this.aggregates, other.aggregates);
    }

    @Override
    public String toString()
    {
        return toStringHelper("query")
                .add("", new SchemaTableName(schemaName, tableName))
                .add("start", timeRangeStart)
                .add("delta", timeRangeDelta)
                .add("window", windowSizeSeconds)
                .add("groupBy", groupBy)
                .add("aggregates", aggregates)
                .omitNullValues()
                .toString();
    }
}

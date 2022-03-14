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

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class BiosQuery
        extends BiosTableHandle
{
    private String[] attributes;
    private BiosAggregate[] aggregates;

    @JsonCreator
    public BiosQuery(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("timeRangeStart") Long timeRangeStart,
            @JsonProperty("timeRangeDelta") Long timeRangeDelta,
            @JsonProperty("windowSize") Long windowSize,
            @JsonProperty("windowSize") String[] groupBy,
            @JsonProperty("attributes") String[] attributes,
            @JsonProperty("aggregates") BiosAggregate[] aggregates)
    {
        super(schemaName, tableName, timeRangeStart, timeRangeDelta, windowSize, groupBy);
        this.attributes = attributes;
        this.aggregates = aggregates;
    }

    public String getUnderlyingTableName()
    {
        if (getTableKind() == BiosTableKind.RAW_SIGNAL) {
            return BiosClient.removeRawSuffix(tableName);
        }
        else {
            return tableName;
        }
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

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), Arrays.hashCode(attributes),
                Arrays.hashCode(aggregates));
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
        if (!super.equals(obj)) {
            return false;
        }

        BiosQuery other = (BiosQuery) obj;
        return Arrays.equals(this.attributes, other.attributes) &&
                Arrays.equals(this.aggregates, other.aggregates);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("super", super.toString())
                .add("attributes", Arrays.toString(attributes))
                .add("aggregates", Arrays.toString(aggregates))
                .omitNullValues()
                .toString();
    }
}

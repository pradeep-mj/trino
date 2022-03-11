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
import static java.util.Objects.requireNonNull;

public final class BiosStatement
{
    private final BiosTableKind tableKind;
    private final String tableName;
    private final String[] attributes;
    private final Long timeRangeStart;
    private final Long timeRangeDelta;

    @JsonCreator
    public BiosStatement(
            @JsonProperty("tableKind") BiosTableKind tableKind,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("attributes") String[] attributes,
            @JsonProperty("timeRangeStart") Long timeRangeStart,
            @JsonProperty("timeRangeDelta") Long timeRangeDelta)
    {
        this.tableKind = tableKind;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.attributes = attributes;
        this.timeRangeStart = timeRangeStart;
        this.timeRangeDelta = timeRangeDelta;
    }

    @JsonProperty
    public BiosTableKind getTableKind()
    {
        return tableKind;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String[] getAttributes()
    {
        return attributes;
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

    @Override
    public int hashCode()
    {
        return Objects.hash(tableKind, tableName, Arrays.hashCode(attributes), timeRangeStart,
                timeRangeDelta);
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

        BiosStatement other = (BiosStatement) obj;
        return Objects.equals(this.tableKind, other.tableKind) &&
                Objects.equals(this.tableName, other.tableName) &&
                Arrays.equals(this.attributes, other.attributes) &&
                Objects.equals(this.timeRangeStart, other.timeRangeStart) &&
                Objects.equals(this.timeRangeDelta, other.timeRangeDelta);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableKind", tableKind)
                .add("tableName", tableName)
                .add("attributes", Arrays.toString(attributes))
                .add("timeRangeStart", timeRangeStart)
                .add("timeRangeDelta", timeRangeDelta)
                .omitNullValues()
                .toString();
    }
}

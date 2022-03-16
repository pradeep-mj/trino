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
{
    private final BiosTableHandle tableHandle;
    private String[] attributes;
    private final BiosAggregate[] aggregates;

    @JsonCreator
    public BiosQuery(
            @JsonProperty("tableHandle") BiosTableHandle tableHandle,
            @JsonProperty("attributes") String[] attributes,
            @JsonProperty("aggregates") BiosAggregate[] aggregates)
    {
        this.tableHandle = tableHandle;
        this.attributes = attributes;
        this.aggregates = aggregates;
    }

    @JsonProperty
    public BiosTableHandle getTableHandle()
    {
        return tableHandle;
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
        return Objects.hash(tableHandle.hashCode(), Arrays.hashCode(attributes),
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

        BiosQuery other = (BiosQuery) obj;
        return Objects.equals(this.tableHandle, other.tableHandle) &&
                Arrays.equals(this.attributes, other.attributes) &&
                Arrays.equals(this.aggregates, other.aggregates);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", tableHandle.toString())
                .add("attributes", Arrays.toString(attributes))
                .add("aggregates", Arrays.toString(aggregates))
                .omitNullValues()
                .toString();
    }
}

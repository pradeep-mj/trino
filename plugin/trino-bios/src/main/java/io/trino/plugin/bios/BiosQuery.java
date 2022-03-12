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
    private final String[] attributes;
    private final String[] keyValues;

    @JsonCreator
    public BiosQuery(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("attributes") String[] attributes,
            @JsonProperty("keyValues") String[] keyValues,
            @JsonProperty("timeRangeStart") Long timeRangeStart,
            @JsonProperty("timeRangeDelta") Long timeRangeDelta)
    {
        super(schemaName, tableName, timeRangeStart, timeRangeDelta);
        this.attributes = attributes;
        this.keyValues = keyValues;
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

    @JsonProperty
    public String[] getKeyValues()
    {
        return keyValues;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), Arrays.hashCode(attributes),
                Arrays.hashCode(keyValues));
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
                Arrays.equals(this.keyValues, other.keyValues);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("super", super.toString())
                .add("attributes", Arrays.toString(attributes))
                .add("keyValues", Arrays.toString(keyValues))
                .omitNullValues()
                .toString();
    }
}

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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class BiosTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final Long timeRangeStart;
    private final Long timeRangeDelta;

    @JsonCreator
    public BiosTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("timeRangeStart") Long timeRangeStart,
            @JsonProperty("timeRangeDelta") Long timeRangeDelta)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.timeRangeStart = timeRangeStart;
        this.timeRangeDelta = timeRangeDelta;
    }

    public BiosTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.timeRangeStart = null;
        this.timeRangeDelta = null;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
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
    public String getTableName()
    {
        return tableName;
    }

    public BiosTableKind getKind()
    {
        if (schemaName.equals("signal")) {
            return BiosTableKind.SIGNAL;
        }
        else {
            return BiosTableKind.CONTEXT;
        }
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName, timeRangeStart, timeRangeDelta);
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
                Objects.equals(this.timeRangeDelta, other.timeRangeDelta);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .add("timeRangeStart", timeRangeStart)
                .add("timeRangeDelta", timeRangeDelta)
                .omitNullValues()
                .toString();
    }
}

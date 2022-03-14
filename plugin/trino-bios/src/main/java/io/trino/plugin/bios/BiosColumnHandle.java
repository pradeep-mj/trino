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
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.bios.BiosClient.VIRTUAL_PREFIX;
import static java.util.Objects.requireNonNull;

public final class BiosColumnHandle
        implements ColumnHandle
{
    private static final Logger logger = Logger.get(BiosColumnHandle.class);

    private final String columnName;
    private final Type columnType;
    private final String defaultValue;
    private final boolean isKey;
    private final String aggregateFunction;
    private final String aggregateSource;

    @JsonCreator
    public BiosColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("defaultValue") String defaultValue,
            @JsonProperty("isKey") boolean isKey,
            @JsonProperty("aggregateFunction") String aggregateFunction,
            @JsonProperty("aggregateSource") String aggregateSource)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.defaultValue = defaultValue;
        this.isKey = isKey;
        this.aggregateFunction = aggregateFunction;
        this.aggregateSource = aggregateSource;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    @JsonProperty
    public String getDefaultValue()
    {
        return defaultValue;
    }

    @JsonProperty
    public boolean getIsKey()
    {
        return isKey;
    }

    @JsonProperty
    public String getAggregateFunction()
    {
        return aggregateFunction;
    }

    @JsonProperty
    public String getAggregateSource()
    {
        return aggregateSource;
    }

    public boolean getIsAggregate()
    {
        return (aggregateFunction != null);
    }

    public boolean getIsVirtual()
    {
        return columnName.startsWith(VIRTUAL_PREFIX);
    }

    public ColumnMetadata getColumnMetadata()
    {
        String extraInfo = null;

        if (isKey) {
            extraInfo = "key";
        }
        final var builder = ColumnMetadata.builder()
                .setName(columnName)
                .setType(columnType)
                .setNullable(false)
                .setExtraInfo(Optional.ofNullable(extraInfo));
        if (defaultValue != null) {
            builder.setComment(Optional.of(String.format("default: %s", defaultValue)));
        }
        if (getIsVirtual()) {
            builder.setComment(Optional.of("virtual column"));
        }

        if (getIsAggregate()) {
            logger.debug("getColumnMetadata called for aggregate %s", columnName);
        }

        return builder.build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnName, columnType, defaultValue, isKey, aggregateFunction,
                aggregateSource);
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

        BiosColumnHandle other = (BiosColumnHandle) obj;
        return Objects.equals(this.columnName, other.columnName) &&
                Objects.equals(this.columnType, other.columnType) &&
                Objects.equals(this.defaultValue, other.defaultValue) &&
                this.isKey == other.isKey &&
                Objects.equals(this.aggregateFunction, other.aggregateFunction) &&
                Objects.equals(this.aggregateSource, other.aggregateSource);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("defaultValue", defaultValue)
                .add("isKey", isKey)
                .add("aggregateFunction", aggregateFunction)
                .add("aggregateSource", aggregateSource)
                .omitNullValues()
                .toString();
    }
}

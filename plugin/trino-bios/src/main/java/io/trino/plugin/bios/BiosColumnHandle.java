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
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class BiosColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private final BiosTableKind tableKind;
    private final boolean isKey;

    @JsonCreator
    public BiosColumnHandle(
            @JsonProperty("columnName") String columnName,
            @JsonProperty("columnType") Type columnType,
            @JsonProperty("tableKind") BiosTableKind tableKind,
            @JsonProperty("isKey") boolean isKey)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.tableKind = requireNonNull(tableKind, "tableKind is null");
        this.isKey = isKey;
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
    public BiosTableKind getTableKind()
    {
        return tableKind;
    }

    @JsonProperty
    public boolean getIsKey()
    {
        return isKey;
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
        // Contexts only support listing the primary key attribute; return null for all columns
        // other than the first one (the primary key).
        if ((tableKind == BiosTableKind.CONTEXT) && !isKey) {
            builder.setNullable(true);
        }

        return builder.build();
    }

    @Override
    public int hashCode()
    {
        return columnName.hashCode();
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
        return columnName.equals(other.columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnName", columnName)
                .add("columnType", columnType)
                .add("isKey", isKey)
                .toString();
    }
}

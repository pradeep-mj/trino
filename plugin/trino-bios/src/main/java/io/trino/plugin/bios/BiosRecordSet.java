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

import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class BiosRecordSet
        implements RecordSet
{
    private final BiosClient biosClient;
    private final BiosTableHandle tableHandle;
    private final List<BiosColumnHandle> columnHandles;
    private final BiosSplit biosSplit;

    public BiosRecordSet(BiosClient biosClient, BiosTableHandle tableHandle,
                         List<BiosColumnHandle> columnHandles, BiosSplit biosSplit)
    {
        this.biosClient = requireNonNull(biosClient, "biosClient is null");
        this.tableHandle = requireNonNull(tableHandle, "statement is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.biosSplit = requireNonNull(biosSplit, "biosSplit is null");
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnHandles.stream()
                .map(BiosColumnHandle::getColumnType)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public RecordCursor cursor()
    {
        return new BiosRecordCursor(biosClient, tableHandle, columnHandles, biosSplit);
    }
}

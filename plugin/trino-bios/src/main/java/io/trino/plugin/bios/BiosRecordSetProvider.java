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

import com.google.inject.Inject;
import io.isima.bios.models.isql.ISqlStatement;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;

public class BiosRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final BiosClient biosClient;

    @Inject
    public BiosRecordSetProvider(BiosClient biosClient)
    {
        this.biosClient = requireNonNull(biosClient, "biosClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        BiosTableHandle tableHandle = (BiosTableHandle) table;

        List<BiosColumnHandle> biosColumnHandles = columns.stream()
                .map(column -> (BiosColumnHandle) column)
                .collect(toUnmodifiableList());
        String[] attributes = biosColumnHandles.stream()
                .map(BiosColumnHandle::getColumnName)
                .toArray(String[]::new);

        // TODO make start and delta dynamically assignable per query.
        long start = System.currentTimeMillis();
        long delta = -(60 * 60 * 1000);

        ISqlStatement statement;
        if (tableHandle.getKind() == BiosTableKind.SIGNAL) {
            statement = ISqlStatement.select(attributes)
                    .from(tableHandle.getTableName())
                    .timeRange(start, delta)
                    .build();
        }
        else {
            statement = ISqlStatement.select(biosColumnHandles.get(0).getColumnName())
                    .fromContext(tableHandle.getTableName())
                    .build();
        }

        return new BiosRecordSet(biosClient, statement, biosColumnHandles);
    }
}

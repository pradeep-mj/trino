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
import io.airlift.log.Logger;
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
    private static final Logger logger = Logger.get(BiosRecordSetProvider.class);

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
        BiosSplit biosSplit = (BiosSplit) split;

        List<BiosColumnHandle> biosColumnHandles = columns.stream()
                .map(column -> (BiosColumnHandle) column)
                .collect(toUnmodifiableList());

        // logger.debug("getRecordSet %s (%s): %s  %s  %s",
        //         tableHandle.toSchemaTableName(), session.getQueryId(),
        //         session.getSource(), session.getStart(), session.getTraceToken());

        return new BiosRecordSet(biosClient, tableHandle, biosColumnHandles, biosSplit);
    }
}

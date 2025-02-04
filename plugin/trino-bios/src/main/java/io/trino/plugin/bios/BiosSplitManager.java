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

import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class BiosSplitManager
        implements ConnectorSplitManager
{
    private static final Logger logger = Logger.get(BiosSplitManager.class);

    private final BiosClient biosClient;

    @Inject
    public BiosSplitManager(BiosClient biosClient)
    {
        this.biosClient = requireNonNull(biosClient, "biosClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        BiosTableHandle tableHandle = (BiosTableHandle) connectorTableHandle;
        var biosConfig = biosClient.getBiosConfig();
        List<ConnectorSplit> splits = new ArrayList<>();

        // Currently, we don't do any splits for contexts.
        if (tableHandle.getTableKind() == BiosTableKind.CONTEXT) {
            splits.add(new BiosSplit(0L, 0L));
            return new FixedSplitSource(splits);
        }

        // logger.debug("getSplits %s %s; dynamicFilter: %s  %s  %s  %s; constraint: %s  %s  %s",
        //         tableHandle.toSchemaTableName(), splitSchedulingStrategy,
        //         dynamicFilter.getColumnsCovered(), dynamicFilter.getCurrentPredicate().toString(),
        //         dynamicFilter.isComplete(), dynamicFilter.isAwaitable(),
        //         constraint.getSummary().toString(), constraint.predicate().toString(),
        //         Arrays.toString(constraint.getPredicateColumns().stream().toArray()));

        // -----------------------------------------------------------------------------------------
        // ** First, normalize time range.

        // * Get the time range requested (implicitly or explicitly) by the user.
        long start = biosClient.getEffectiveTimeRangeStart(tableHandle);
        long delta = biosClient.getEffectiveTimeRangeDelta(tableHandle);

        // * Remove future times if present.
        long currentTimeWithLag = biosClient.getCurrentTimeWithLag(tableHandle);
        if (start > currentTimeWithLag - delta) {
            start = currentTimeWithLag - delta;
        }
        long end = start + delta;

        // -----------------------------------------------------------------------------------------
        // ** Next, create splits with the ranges that we want to use for actual queries again bios.

        // * Get split size and align it to window size.
        long windowSizeSeconds = biosClient.getEffectiveWindowSizeSeconds(tableHandle);
        long splitSize = (tableHandle.getTableKind() == BiosTableKind.SIGNAL) ?
                biosConfig.getFeatureSplitSizeSeconds() * 1000 :
                biosConfig.getRawSignalSplitSizeSeconds() * 1000;
        splitSize = BiosClient.ceiling(splitSize, windowSizeSeconds * 1000);

        // * Create splits.
        // All splits except possibly the latest one should be perfectly aligned to split size.
        // The latest split should be smaller than the full split size if any part of it is in the
        // future. When deciding what is in the future, also consider the lag time.

        long nextStart = BiosClient.floor(start, splitSize);
        while (nextStart < end) {
            long currentSplitSize;
            if (nextStart + splitSize <= currentTimeWithLag) {
                // This split does not have any part in the future (with lag).
                currentSplitSize = splitSize;
            }
            else {
                // This split has some part in the future. Limit it to the current time (with lag)
                // to avoid caching non-existent rows for future times.
                // To get good caching, also align it to a minimum alignment size.
                final var minimumAlignment =
                        (tableHandle.getTableKind() == BiosTableKind.RAW_SIGNAL) ?
                                biosConfig.getRawSignalAlignmentSeconds() * 1000 :
                                biosConfig.getFeatureAlignmentSeconds() * 1000;
                currentSplitSize = BiosClient.floor(end - nextStart, minimumAlignment);
                if (currentSplitSize == 0) {
                    break;
                }
            }
            splits.add(new BiosSplit(nextStart, currentSplitSize));
            nextStart += currentSplitSize;
        }

        return new FixedSplitSource(splits);
    }
}

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

import java.util.Objects;

public final class BiosAggregate
{
    private static final Logger logger = Logger.get(BiosAggregate.class);

    private final String aggregateFunction;
    private final String aggregateSource;

    @JsonCreator
    public BiosAggregate(
            @JsonProperty("aggregateFunction") String aggregateFunction,
            @JsonProperty("aggregateSource") String aggregateSource)
    {
        this.aggregateFunction = aggregateFunction;
        this.aggregateSource = aggregateSource;
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

    @Override
    public int hashCode()
    {
        return Objects.hash(aggregateFunction, aggregateSource);
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

        BiosAggregate other = (BiosAggregate) obj;
        return Objects.equals(this.aggregateFunction, other.aggregateFunction) &&
                Objects.equals(this.aggregateSource, other.aggregateSource);
    }

    @Override
    public String toString()
    {
        String source = aggregateSource != null ? aggregateSource : "";
        return aggregateFunction + "(" + source + ")";
    }
}

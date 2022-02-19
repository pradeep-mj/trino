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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class BiosClient
{
    private static final Logger logger = Logger.get(BiosClient.class);

    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, Map<String, BiosTable>>> schemas;

    @Inject
    public BiosClient(BiosConfig config, JsonCodec<Map<String, List<BiosTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        schemas = Suppliers.memoize(schemasSupplier(catalogCodec, config.getUrl(),
                config.getEmail(), config.getPassword()));
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        Map<String, BiosTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return ImmutableSet.of();
        }
        return tables.keySet();
    }

    public BiosTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        Map<String, BiosTable> tables = schemas.get().get(schema);
        if (tables == null) {
            return null;
        }
        return tables.get(tableName);
    }

    private static Supplier<Map<String, Map<String, BiosTable>>> schemasSupplier(JsonCodec<Map<String, List<BiosTable>>> catalogCodec, URI url, String email, String password)
    {
        return () -> {
            try {
                return lookupSchemas(url, email, password, catalogCodec);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static Map<String, Map<String, BiosTable>> lookupSchemas(URI url, String email, String password, JsonCodec<Map<String, List<BiosTable>>> catalogCodec)
            throws IOException
    {
        logger.debug(catalogCodec.toString());

        Map<String, Map<String, BiosTable>> schemas = new HashMap<>();
        Map<String, BiosTable> tables = new HashMap<>();

        tables.put("dummyTable1", new BiosTable("dummyTable1"));
        schemas.put("dummySchema", tables);

        return ImmutableMap.copyOf(schemas);
    }
}

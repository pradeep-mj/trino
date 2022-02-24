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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class BiosClient
{
    private static final Logger logger = Logger.get(BiosClient.class);

    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Session> session;
    private final URI url;
    private final String email;
    private final String password;

    @Inject
    public BiosClient(BiosConfig config, JsonCodec<Map<String, List<BiosTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        url = config.getUrl();
        requireNonNull(url, "url is null");

        email = config.getEmail();
        password = config.getPassword();
        checkArgument(!isNullOrEmpty(email), "email is null");
        checkArgument(!isNullOrEmpty(password), "password is null");

        session = Suppliers.memoize(sessionSupplier(url, email, password));
    }

    private static Supplier<Session> sessionSupplier(URI url, String email, String password)
    {
        return () -> {
            Session session;
            try {
                session = Bios.newSession(url.getHost(), url.getPort())
                        .user(email)
                        .password(password)
                        .sslCertFile(null)
                        .connect();
                return session;
            }
            catch (BiosClientException e) {
                throw new RuntimeException(e.toString());
            }
        };
    }

    public List<String> getSchemaNames()
    {
        return ImmutableList.of("context", "signal");
    }

    public Set<String> getTableNames(String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");
        // session.get(). TODO
        if (schemaName.equals("signal")) {
            return ImmutableSet.of("signal1");
        }
        else if (schemaName.equals("context")) {
            return ImmutableSet.of("context1");
        }
        return null;
    }

    public BiosTable getTable(String schemaName, String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        BiosTableHandle tableHandle = new BiosTableHandle(schemaName, tableName);
        List<BiosColumnHandle> columns = ImmutableList.of(
                new BiosColumnHandle("stringCol", VARCHAR, false),
                new BiosColumnHandle("intCol", BIGINT, false));
        // session.get(). TODO
        if (schemaName.equals("signal")) {
            return new BiosTable(BiosTableKind.SIGNAL, tableHandle, columns);
        }
        else if (schemaName.equals("context")) {
            return new BiosTable(BiosTableKind.CONTEXT, tableHandle, columns);
        }
        return null;
    }
}

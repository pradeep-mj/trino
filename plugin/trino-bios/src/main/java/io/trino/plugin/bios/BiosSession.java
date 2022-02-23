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

import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.exceptions.BiosClientException;

public final class BiosSession
{
    private static Session session;

    private BiosSession()
    {
        // never called
    }

    protected static Session createSession(final String host, final String email, final String password)
            throws BiosClientException
    {
        session = Bios.newSession(host, 443)
                .user(email)
                .password(password)
                .sslCertFile(null)
                .connect();
        return session;
    }

    protected static Session getSession()
    {
        return session;
    }
}

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

import io.isima.bios.sdk.Session;

import java.security.Principal;

import static com.google.common.base.MoreObjects.toStringHelper;

public class BiosPrincipal
        implements Principal
{
    private final String email;
    private final Session session;

    public BiosPrincipal(String email, Session session)
    {
        this.email = email;
        this.session = session;
    }

    @Override
    public String getName()
    {
        return email;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("email", email)
                .toString();
    }

    public Session getSession()
    {
        return session;
    }
}

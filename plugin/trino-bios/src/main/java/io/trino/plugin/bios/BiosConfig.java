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

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.net.URI;

public class BiosConfig
{
    private URI url;
    private String email;
    private String password;
    private Long defaultTimeRangeDeltaSeconds;

    @NotNull
    public URI getUrl()
    {
        return url;
    }

    @Config("bios.url")
    public BiosConfig setUrl(URI url)
    {
        this.url = url;
        return this;
    }

    @NotNull
    public String getEmail()
    {
        return email;
    }

    @Config("bios.email")
    public BiosConfig setEmail(String email)
    {
        this.email = email;
        return this;
    }

    @NotNull
    public String getPassword()
    {
        return password;
    }

    @Config("bios.password")
    public BiosConfig setPassword(String password)
    {
        this.password = password;
        return this;
    }

    @NotNull
    public Long getDefaultTimeRangeDeltaSeconds()
    {
        return defaultTimeRangeDeltaSeconds;
    }

    @Config("bios.defaultTimeRangeDeltaSeconds")
    public BiosConfig setDefaultTimeRangeDeltaSeconds(Long defaultTimeRangeDeltaSeconds)
    {
        this.defaultTimeRangeDeltaSeconds = defaultTimeRangeDeltaSeconds;
        return this;
    }
}

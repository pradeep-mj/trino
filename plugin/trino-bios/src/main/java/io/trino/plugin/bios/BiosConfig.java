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
    private Long metadataCacheSeconds;
    private Long dataCacheSeconds;
    private Long dataCacheSizeInRows;
    private Long defaultTimeRangeDeltaSeconds;
    private Long defaultWindowSizeSeconds;
    private Long rawSignalLagSeconds;
    private Long featureLagSeconds;
    private Long rawSignalSplitSizeSeconds;
    private Long featureSplitSizeSeconds;
    private Long dataAlignmentSeconds;

    public URI getUrl()
    {
        return url != null ? url : URI.create("https://bios.isima.io");
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

    public Long getMetadataCacheSeconds()
    {
        return metadataCacheSeconds != null ? metadataCacheSeconds : 15 * 60L;
    }

    @Config("bios.metadataCacheSeconds")
    public BiosConfig setMetadataCacheSeconds(Long metadataCacheSeconds)
    {
        this.metadataCacheSeconds = metadataCacheSeconds;
        return this;
    }

    public Long getDataCacheSeconds()
    {
        return dataCacheSeconds != null ? dataCacheSeconds : 15 * 60L;
    }

    @Config("bios.dataCacheSeconds")
    public BiosConfig setDataCacheSeconds(Long dataCacheSeconds)
    {
        this.dataCacheSeconds = dataCacheSeconds;
        return this;
    }

    public Long getDataCacheSizeInRows()
    {
        return dataCacheSizeInRows != null ? dataCacheSizeInRows : 1000 * 1000L;
    }

    @Config("bios.dataCacheSizeInRows")
    public BiosConfig setDataCacheSizeInRows(Long dataCacheSizeInRows)
    {
        this.dataCacheSizeInRows = dataCacheSizeInRows;
        return this;
    }

    public Long getDefaultTimeRangeDeltaSeconds()
    {
        return defaultTimeRangeDeltaSeconds != null ? defaultTimeRangeDeltaSeconds : 15 * 60L;
    }

    @Config("bios.defaultTimeRangeDeltaSeconds")
    public BiosConfig setDefaultTimeRangeDeltaSeconds(Long defaultTimeRangeDeltaSeconds)
    {
        this.defaultTimeRangeDeltaSeconds = defaultTimeRangeDeltaSeconds;
        return this;
    }

    public Long getDefaultWindowSizeSeconds()
    {
        return defaultWindowSizeSeconds != null ? defaultWindowSizeSeconds : 5 * 60L;
    }

    @Config("bios.defaultWindowSizeSeconds")
    public BiosConfig setDefaultWindowSizeSeconds(Long defaultWindowSizeSeconds)
    {
        this.defaultWindowSizeSeconds = defaultWindowSizeSeconds;
        return this;
    }

    public Long getRawSignalLagSeconds()
    {
        return rawSignalLagSeconds != null ? rawSignalLagSeconds : 2L;
    }

    @Config("bios.rawSignalLagSeconds")
    public BiosConfig setRawSignalLagSeconds(Long rawSignalLagSeconds)
    {
        this.rawSignalLagSeconds = rawSignalLagSeconds;
        return this;
    }

    public Long getFeatureLagSeconds()
    {
        return featureLagSeconds != null ? featureLagSeconds : 40L;
    }

    @Config("bios.featureLagSeconds")
    public BiosConfig setFeatureLagSeconds(Long featureLagSeconds)
    {
        this.featureLagSeconds = featureLagSeconds;
        return this;
    }

    public Long getRawSignalSplitSizeSeconds()
    {
        return rawSignalSplitSizeSeconds != null ? rawSignalSplitSizeSeconds : 5 * 60L;
    }

    @Config("bios.rawSignalSplitSizeSeconds")
    public BiosConfig setRawSignalSplitSizeSeconds(Long rawSignalSplitSizeSeconds)
    {
        this.rawSignalSplitSizeSeconds = rawSignalSplitSizeSeconds;
        return this;
    }

    public Long getFeatureSplitSizeSeconds()
    {
        return featureSplitSizeSeconds != null ? featureSplitSizeSeconds : 24 * 60 * 60L;
    }

    @Config("bios.featureSplitSizeSeconds")
    public BiosConfig setFeatureSplitSizeSeconds(Long featureSplitSizeSeconds)
    {
        this.featureSplitSizeSeconds = featureSplitSizeSeconds;
        return this;
    }

    public Long getDataAlignmentSeconds()
    {
        return dataAlignmentSeconds != null ? dataAlignmentSeconds : 5 * 60L;
    }

    @Config("bios.dataAlignmentSeconds")
    public BiosConfig setDataAlignmentSeconds(Long dataAlignmentSeconds)
    {
        this.dataAlignmentSeconds = dataAlignmentSeconds;
        return this;
    }
}

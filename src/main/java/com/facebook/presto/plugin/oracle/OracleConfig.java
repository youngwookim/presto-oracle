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
package com.facebook.presto.plugin.oracle;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import io.airlift.configuration.Config;

/**
 * To get the custom properties to connect to the database. User, password and
 * URL is provided by de BaseJdbcClient is not required. If there is another
 * custom configuration it should be put in here.
 *
 * @author Marcelo Paes Rech
 *
 */
public class OracleConfig extends BaseJdbcConfig
{
    private String defaultRowPrefetch;

    @Config("oracle.defaultRowPrefetch")
    public OracleConfig setDefaultRowPrefetch(String defaultRowPrefetch)
    {
        this.defaultRowPrefetch = defaultRowPrefetch;
        return this;
    }

    public String getDefaultRowPrefetch()
    {
        return defaultRowPrefetch;
    }
}

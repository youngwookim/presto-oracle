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

import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.log.Logger;
import oracle.jdbc.OracleDriver;

import javax.inject.Inject;

import java.sql.SQLException;
import java.sql.Types;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.min;

/**
 * Implementation of OracleClient. It describes table, schemas and columns
 * behaviours. It allows to change the QueryBuilder to a custom one as well.
 *
 * @author Marcelo Paes Rech
 *
 */
public class OracleClient extends BaseJdbcClient
{
    private static final Logger log = Logger.get(OracleClient.class);

    @Inject
    public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config, OracleConfig oracleConfig)
            throws SQLException
    {
        // https://docs.oracle.com/cd/B28359_01/server.111/b28286/sql_elements008.htm
        super(connectorId, oracleConfig, "\"", new OracleDriver());

        if (oracleConfig.getDefaultRowPrefetch() != null) {
            connectionProperties.setProperty("defaultRowPrefetch", oracleConfig.getDefaultRowPrefetch());
        }
        else {
            connectionProperties.setProperty("defaultRowPrefetch", "10000");
        }
    }

    @Override
    protected Type toPrestoType(int jdbcType, int columnSize)
    {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return INTEGER;
            case Types.BIGINT:
                return BIGINT;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return DOUBLE;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return createVarcharType(min(columnSize, VarcharType.MAX_LENGTH));
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return VARBINARY;
            case Types.DATE:
                return DATE;
            case Types.TIME:
                return TIME;
            case Types.TIMESTAMP:
                return TIMESTAMP;
            case Types.BLOB:
                return VARBINARY;
            case Types.OTHER:
                return VARBINARY;
        }
        return null;
    }
}

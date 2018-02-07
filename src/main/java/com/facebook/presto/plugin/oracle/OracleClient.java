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
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.log.Logger;
import oracle.jdbc.OracleDriver;

import javax.inject.Inject;

import java.sql.SQLException;
import java.sql.Types;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.CharType.createCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class OracleClient
        extends BaseJdbcClient
{
    private static final Logger LOG = Logger.get(OracleClient.class);

    @Inject
    public OracleClient(JdbcConnectorId connectorId, BaseJdbcConfig config)
            throws SQLException
    {
        super(connectorId, config, "\"", new DriverConnectionFactory(new OracleDriver(), config));
    }
    
    @Override
    protected Type toPrestoType(int jdbcType, int columnSize, int decimalDigits)
    {
        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                return BOOLEAN;
            case Types.TINYINT:
                return TINYINT;
            case Types.SMALLINT:
                return SMALLINT;
            case Types.INTEGER:
                return INTEGER;
            case Types.BIGINT:
                return BIGINT;
            case Types.REAL:
                return REAL;
            case Types.FLOAT:
            case Types.DOUBLE:
                return DOUBLE;
            case Types.NUMERIC:
            case Types.DECIMAL:
                int precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                if (precision > Decimals.MAX_PRECISION) {
                    return null;
                }
                return createDecimalType(precision, max(decimalDigits, 0));
            case Types.CHAR:
            case Types.NCHAR:
                return createCharType(min(columnSize, CharType.MAX_LENGTH));
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return createUnboundedVarcharType();
                }
                return createVarcharType(columnSize);
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

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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.QueryBuilder;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class OracleQueryBuilder extends QueryBuilder
{
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public OracleQueryBuilder(String quote)
    {
        super(quote);
    }

    @Override
    protected String toPredicate(String columnName, String operator, Object value, Type type)
    {
        // QueryBuilder can not generate SQL expression for Oracle's Timestamp
        String valueStrting;
        if (type.equals(TimestampType.TIMESTAMP)) {
            long unixtime = Longs.tryParse(value.toString());
            valueStrting = "TO_TIMESTAMP(" + encode(timestampFormat.format(new Date(unixtime)))
                    + ", 'yyyy-mm-dd hh24:mi:ss')";
        }
        else if (type.equals(DateType.DATE)) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(0);
            c.add(Calendar.DATE, Ints.tryParse(value.toString()));
            valueStrting = encode(dateFormat.format(c.getTime()));
        }
        else if (type.equals(VarcharType.VARCHAR)) {
            valueStrting = encode(value);
        }
        else {
            valueStrting = encode(value);
        }
        return quote(columnName) + " " + operator + " " + valueStrting;
    }
    
    private List<String> toConjuncts(List<JdbcColumnHandle> columns, TupleDomain<ColumnHandle> tupleDomain)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        for (JdbcColumnHandle column : columns) {
            Type type = column.getColumnType();

            if (type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE) || type.equals(BooleanType.BOOLEAN) ||
                    type.equals(VarcharType.VARCHAR) || type.equals(DateType.DATE) || type.equals(TimestampType.TIMESTAMP)) {
                Domain domain = tupleDomain.getDomains().get().get(column);
                if (domain != null) {
                    String predicate = toPredicate(column.getColumnName(), domain, type);
                    builder.add(predicate);
                }
            }
        }
        return builder.build();
    }
}

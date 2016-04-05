/*
 *  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.das.jdbcdriver.aggregateFunctions;

import org.wso2.das.jdbcdriver.expressions.Expression;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class which implements the SQL SUM Aggregation function
 */
public class SumAggrFunction extends AggregateFunction {

    Expression expression;
    BigDecimal sum = null;

    public SumAggrFunction(Expression expression)
    {
        this.expression = expression;
    }

    /**
     * Evaluate each row against the function
     * @param env RecordEnvironment which contains the data
     * @throws SQLException
     */
    @Override
    public void processRow(Map<String, Object> env) throws SQLException {

        Object o = expression.eval(env);
        if (o != null)
        {
            //TODO:Add support for distinct count
            try
            {
                if (sum == null)
                    sum = new BigDecimal(o.toString());
                else
                    sum = sum.add(new BigDecimal(o.toString()));
            }
            catch (NumberFormatException e)
            {
            }
        }
    }

    public List<String> getFilteredColumns(Set<String> availableColumns){
        return new LinkedList<String>();
    }

    /**
     * Get the evaluated result of the function with the given data.
     * @param env - RecordEnvironment which contains the details of the record
     * @throws SQLException
     */
    public Object eval(Map<String, Object> env) throws SQLException
    {
        Object retval = null;
        try {
            if (sum != null)
                retval = Long.valueOf(sum.longValueExact());
        }
        catch (ArithmeticException e){
            retval = sum.doubleValue();
        }

        return retval;
    }
}

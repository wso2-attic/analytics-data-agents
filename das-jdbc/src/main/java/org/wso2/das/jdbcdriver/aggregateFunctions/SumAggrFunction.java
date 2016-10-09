/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.das.jdbcdriver.aggregateFunctions;

import org.wso2.das.jdbcdriver.expressions.Expression;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class which implements the SQL SUM Aggregation function.
 */
public class SumAggrFunction extends AggregateFunction {

    private Expression expression;
    private BigDecimal sum = null;
    private static Logger logger = Logger.getLogger(SumAggrFunction.class.getName());

    public SumAggrFunction(Expression expression) {
        this.expression = expression;
    }

    /**
     * Evaluate each row against the function.
     *
     * @param env RecordEnvironment which contains the data
     */
    @Override public void processRow(Map<String, Object> env) {
        Object o = this.expression.eval(env);
        if (o != null) {
            try {
                if (this.sum == null) {
                    this.sum = new BigDecimal(o.toString());
                } else {
                    this.sum = this.sum.add(new BigDecimal(o.toString()));
                }
            } catch (NumberFormatException e) {
                logger.log(Level.SEVERE, "Error in Process Sum:", e);
            }
        }
    }

    /**
     * Get the evaluated result of the function with the given data.
     *
     * @param env - RecordEnvironment which contains the details of the record
     */
    public Object eval(Map<String, Object> env) {
        Object retVal = null;
        try {
            if (this.sum != null) {
                retVal = this.sum.longValueExact();
            }
        } catch (ArithmeticException e) {
            retVal = this.sum.doubleValue();
        }
        return retVal;
    }
}

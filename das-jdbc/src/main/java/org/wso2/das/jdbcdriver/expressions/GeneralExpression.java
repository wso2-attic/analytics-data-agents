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
package org.wso2.das.jdbcdriver.expressions;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Expression which holds key value pairs of other expressions
 */
public class GeneralExpression extends Expression {

    String key;
    Expression expression;

    public GeneralExpression(String fieldName, Expression exp)
    {
        this.key = fieldName.toUpperCase();
        this.expression = exp;
    }
    public Object eval(Map<String, Object> env) throws SQLException
    {
        return expression.eval(env);
    }
    public String toString()
    {
        return key+": "+expression.toString();
    }

    public List<String> getFilteredColumns(Set<String> availableColumns)
    {
        return expression.getFilteredColumns(availableColumns);
    }
}

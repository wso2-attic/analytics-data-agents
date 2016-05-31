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
package org.wso2.das.jdbcdriver.expressions;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * Class which represents the OR operation of relational expressions
 */
public class AndExpression extends Expression {

    Vector<Expression> vectorExpressions;

    public AndExpression(Vector<Expression> vectorExpressions) {
        this.vectorExpressions = vectorExpressions;
    }

    /**
     * Evaluates to true all of the expression is true
     *
     * @param env record data
     * @return Boolean values which indicates whether the expression is true
     */
    public Boolean isTrue(Map<String, Object> env) {
        Boolean bIsTrue = Boolean.FALSE;
        for (Expression expr : vectorExpressions) {
            bIsTrue = expr.isTrue(env);
            if (!bIsTrue) {
                break;
            }
        }
        return bIsTrue;
    }

    public List<String> getFilteredColumns(Set<String> availableColumns) {
        List<String> result = new LinkedList<String>();
        for (Expression expr : vectorExpressions) {
            result.addAll(expr.getFilteredColumns(availableColumns));
        }
        return result;
    }
}

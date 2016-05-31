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

/**
 * Represents the Relational Operation and methods to evaluate relational operations
 */
public class RelationOpExpression extends Expression {

    String op;
    Expression left, right;

    public RelationOpExpression(String op, Expression left, Expression right) {
        this.op = op;
        this.left = left;
        this.right = right;
    }

    /**
     * Evalutate whether the Relational operation is true for a given record
     * @param env - Record details
     * @return Boolean value indicating evaluation result
     */
    public Boolean isTrue(Map<String, Object> env) {
        Boolean result = null;
        Comparable leftValue = (Comparable) left.eval(env);
        Comparable rightValue = (Comparable) right.eval(env);
        Integer leftComparedToRightObj = compare(leftValue, rightValue);
        if (leftComparedToRightObj != null) {
            int leftComparedToRight = leftComparedToRightObj;
            if (leftValue != null && rightValue != null) {
                if (op.equals("=")) {
                    result = leftComparedToRight == 0;
                } else if (op.equals("<>") || op.equals("!=")) {
                    result = leftComparedToRight != 0;
                } else if (op.equals(">")) {
                    result = leftComparedToRight > 0;
                } else if (op.equals("<")) {
                    result = leftComparedToRight < 0;
                } else if (op.equals("<=") || op.equals("=<")) {
                    result = leftComparedToRight <= 0;
                } else if (op.equals(">=") || op.equals("=>")) {
                    result = leftComparedToRight >= 0;
                }
            }
        }
        return result;
    }

    public Integer compare(Comparable leftValue, Comparable rightValue) {
        Integer leftComparedToRightObj = null;
        try {
            if (leftValue == null || rightValue == null) {
                leftComparedToRightObj = null;
            } else if (leftValue instanceof String) {
                leftComparedToRightObj = leftValue.compareTo(rightValue);
            } else if (leftValue instanceof Boolean) {
                Boolean leftBoolean = (Boolean) leftValue;
                Boolean rightBoolean = Boolean.valueOf(rightValue.toString());

                if (leftBoolean.equals(rightBoolean))
                {
                    leftComparedToRightObj = 0;
                } else if (!leftBoolean) {
                    leftComparedToRightObj = -1;
                } else {
                    leftComparedToRightObj = 1;
                }
            } else {
                Double leftDouble = new Double((leftValue).toString());
                Double rightDouble = Double.parseDouble(rightValue.toString());
                leftComparedToRightObj = leftDouble.compareTo(rightDouble);
            }

        } catch (ClassCastException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        return leftComparedToRightObj;
    }

    public List<String> getFilteredColumns(Set<String> availableColumns) {
        List<String> result = new LinkedList<String>();
        result.addAll(left.getFilteredColumns(availableColumns));
        result.addAll(right.getFilteredColumns(availableColumns));
        return result;
    }
}

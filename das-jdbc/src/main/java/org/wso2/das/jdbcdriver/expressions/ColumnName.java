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
 * Expression which represents the Column.
 */
public class ColumnName extends Expression {

    private String columnName;

    public ColumnName(String columnName) {
        this.columnName = columnName.toUpperCase();
    }

    public Object eval(Map<String, Object> env) {
        return env.get(this.columnName);
    }

    public String toString() {
        return "[" + this.columnName + "]";
    }

    public List<String> getFilteredColumns(Set<String> availableColumns) {
        List<String> result = new LinkedList<String>();
        result.add(this.columnName);
        return result;
    }
}

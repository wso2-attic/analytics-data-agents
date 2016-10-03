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
package org.wso2.das.jdbcdriver.common;

import org.gibello.zql.ParseException;
import org.gibello.zql.ZExpression;
import org.gibello.zql.ZFromItem;
import org.gibello.zql.ZQuery;
import org.gibello.zql.ZSelectItem;
import org.gibello.zql.ZStatement;
import org.gibello.zql.ZqlParser;
import org.wso2.das.jdbcdriver.aggregateFunctions.MaxAggrFunction;
import org.wso2.das.jdbcdriver.aggregateFunctions.MinAggrFunciton;
import org.wso2.das.jdbcdriver.aggregateFunctions.SumAggrFunction;
import org.wso2.das.jdbcdriver.aggregateFunctions.CountAggrFunction;
import org.wso2.das.jdbcdriver.expressions.AndExpression;
import org.wso2.das.jdbcdriver.expressions.AsteriskExpression;
import org.wso2.das.jdbcdriver.expressions.ColumnName;
import org.wso2.das.jdbcdriver.expressions.Expression;
import org.wso2.das.jdbcdriver.expressions.GeneralExpression;
import org.wso2.das.jdbcdriver.expressions.ORExpression;
import org.wso2.das.jdbcdriver.expressions.RelationOpExpression;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * This class have the functions required for parsing the SQL. ZQL Sql parser is used for this.
 */
public class SQLParser {

    private List<Object[]> queryEnvironment = new ArrayList<Object[]>();
    private Expression whereExpression;
    private String sTableName;
    private String sql;

    public SQLParser(String sql) {
        this.sql = sql;
    }

    /**
     * Parse the SQL Query String by using the ZQL parser.
     */
    public void parse() throws SQLException {
        System.out.println("SQL:" + sql);
        if (sql.length() > 0) {
            if (sql.charAt(sql.length() - 1) != ';') {
                sql = sql.concat(";");
            }
        }
        InputStream is = new ByteArrayInputStream(sql.getBytes());
        ZqlParser p = new ZqlParser(new DataInputStream(is));
        ZStatement st = null;
        try {
            st = p.readStatement();
        } catch (ParseException e) {
            throw new SQLException("Error in Parsing SQL:",e);
        }
        if (st != null) {
            if (st instanceof ZQuery) {
                ZQuery q = (ZQuery) st;
                Vector sel = q.getSelect(); // SELECT part of the query
                Vector from = q.getFrom();  // FROM part of the query
                ZExpression where = (ZExpression) q.getWhere();  // WHERE part of the query
                if (where != null) {
                    this.whereExpression = getWhereClauseExpression(where);
                }
                if (from.size() > 1) {
                    throw new SQLException("Joins are not supported");
                }
                // Retrieve the table name in the FROM clause
                ZFromItem table = (ZFromItem) from.elementAt(0);
                this.sTableName = table.getTable();
                if (sel.size() == 0) {
                    this.queryEnvironment.add(new Object[] { "*", new AsteriskExpression("*") });
                } else {
                    for (int i = 0; i < sel.size(); i++) {
                        ZSelectItem item = (ZSelectItem) sel.elementAt(i);
                        String columnName = item.getColumn();
                        String aggragateName = item.getAggregate();
                        if (aggragateName != null) {
                            Expression funcExpression;
                            if (columnName.equals("*")) {
                                funcExpression = new AsteriskExpression("*");
                            } else {
                                funcExpression = new ColumnName(columnName);
                            }
                            Expression aggregateExpression = getAggregateFunctionExpression(aggragateName,
                                    funcExpression);
                            this.queryEnvironment.add(new Object[] { columnName, aggregateExpression });
                        } else {
                            if (columnName.equals("*")) {
                                this.queryEnvironment
                                        .add(new Object[] { columnName, new AsteriskExpression(columnName) });
                            } else {
                                if (columnName
                                        .startsWith(ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_NAME + ".")) {
                                    columnName = columnName.substring(
                                            ServiceConstants.DAS_SERVICE_QUERIES.DAS_SCHEMA_NAME.length() + 1);
                                }
                                if (columnName.startsWith(this.sTableName + ".")) {
                                    columnName = columnName.substring(this.sTableName.length() + 1);
                                }
                                if (columnName.contains(".")) {
                                    columnName = columnName.split("\\.")[1];
                                }
                                this.queryEnvironment.add(new Object[] { columnName,
                                        new GeneralExpression(columnName, new ColumnName(columnName)) });
                            }
                        }
                    }
                }
            }
        }
    }

    public List<Object[]> getQueryEnvironment() {
        return this.queryEnvironment;
    }

    public String getTableName() {
        return this.sTableName;
    }

    public Expression getWhereExpression() {
        return this.whereExpression;
    }

    /**
     * Get the Aggregate Funtion from the ZQL expression.
     *
     * @param aggregateFuncName  Name of the Aggreate Function -COUNT|MIN|MAX|SUM
     * @param internalExpression ZQL expression which contains the aggragate function
     */
    private Expression getAggregateFunctionExpression(String aggregateFuncName, Expression internalExpression) {
        if (aggregateFuncName.equalsIgnoreCase(ServiceConstants.AGGREGATE_FUNCTIONS.AGGR_FUNC_COUNT)) {
            return new CountAggrFunction(internalExpression);
        } else if (aggregateFuncName.equalsIgnoreCase(ServiceConstants.AGGREGATE_FUNCTIONS.AGGR_FUNC_SUM)) {
            return new SumAggrFunction(internalExpression);
        } else if (aggregateFuncName.equalsIgnoreCase(ServiceConstants.AGGREGATE_FUNCTIONS.AGGR_FUNC_MIN)) {
            return new MinAggrFunciton(internalExpression);
        } else if (aggregateFuncName.equalsIgnoreCase(ServiceConstants.AGGREGATE_FUNCTIONS.AGGR_FUNC_MAX)) {
            return new MaxAggrFunction(internalExpression);
        } else {
            return null;
        }
    }

    /**
     * Get the where clause from the given ZQL expression.
     *
     * @param where ZQL Expression which contains the where clause
     */
    private Expression getWhereClauseExpression(ZExpression where) {
        Expression expr;
        String sOperator = where.getOperator();
        if (sOperator.equalsIgnoreCase(ServiceConstants.OPERATORS.OPERATOR_AND)) {
            Vector<ZExpression> operands = where.getOperands();
            Vector<Expression> vecExpressions = new Vector<Expression>();
            for (ZExpression zExp : operands) {
                RelationOpExpression relOperation = getRelationOpExpression(zExp);
                vecExpressions.add(relOperation);
            }
            expr = new AndExpression(vecExpressions);
        } else if (sOperator.equalsIgnoreCase(ServiceConstants.OPERATORS.OPERATOR_OR)) {
            Vector<ZExpression> operands = where.getOperands();
            Vector<Expression> vecExpressions = new Vector<Expression>();
            for (ZExpression zExp : operands) {
                RelationOpExpression relOperation = getRelationOpExpression(zExp);
                vecExpressions.add(relOperation);
            }
            expr = new ORExpression(vecExpressions);
        } else {
            expr = getRelationOpExpression(where);
        }
        return expr;
    }

    /**
     * Construct the Relation Operation Expression.
     *
     * @param zExp ZQL expression which represents a relation operation
     */
    private RelationOpExpression getRelationOpExpression(ZExpression zExp) {
        String sColName = zExp.getOperand(0).toString();
        String sColVal = zExp.getOperand(1).toString();
        if (sColVal.startsWith("'")) {
            sColVal = sColVal.substring(1);
        }
        if (sColVal.endsWith("'")) {
            sColVal = sColVal.substring(0, sColVal.length() - 1);
        }
        return new RelationOpExpression(zExp.getOperator(), new ColumnName(sColName),new AsteriskExpression(sColVal));
    }
}
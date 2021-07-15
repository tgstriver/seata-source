/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.datasource.exec;

import io.seata.common.DefaultValues;
import io.seata.common.util.IOUtil;
import io.seata.common.util.StringUtils;
import io.seata.config.Configuration;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.rm.datasource.ColumnUtils;
import io.seata.rm.datasource.SqlGenerateUtils;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.sqlparser.ParametersHolder;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.SQLUpdateRecognizer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * The type Update executor.
 *
 * @param <T> the type parameter
 * @param <S> the type parameter
 * @author sharajava
 */
public class UpdateExecutor<T, S extends Statement> extends AbstractDMLBaseExecutor<T, S> {

    private static final Configuration CONFIG = ConfigurationFactory.getInstance();

    private static final boolean ONLY_CARE_UPDATE_COLUMNS = CONFIG.getBoolean(
            ConfigurationKeys.TRANSACTION_UNDO_ONLY_CARE_UPDATE_COLUMNS, DefaultValues.DEFAULT_ONLY_CARE_UPDATE_COLUMNS);

    /**
     * Instantiates a new Update executor.
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public UpdateExecutor(StatementProxy<S> statementProxy, StatementCallback<T, S> statementCallback,
                          SQLRecognizer sqlRecognizer) {
        super(statementProxy, statementCallback, sqlRecognizer);
    }

    @Override
    protected TableRecords beforeImage() throws SQLException {
        //预编译参数列表集合
        ArrayList<List<Object>> paramAppenderList = new ArrayList<>();
        //获取我们的表结构
        TableMeta tmeta = getTableMeta();
        //构建前置镜像sql
        String selectSQL = buildBeforeImageSQL(tmeta, paramAppenderList);
        //执行select id,count from product where product_id=? for update
        //返回前置镜像
        return buildTableRecords(tmeta, selectSQL, paramAppenderList);
    }

    private String buildBeforeImageSQL(TableMeta tableMeta, ArrayList<List<Object>> paramAppenderList) {
        //sql识别器
        SQLUpdateRecognizer recognizer = (SQLUpdateRecognizer) sqlRecognizer;
        //获取更新的字段数
        List<String> updateColumns = recognizer.getUpdateColumns();
        //拼接select查询关键字
        StringBuilder prefix = new StringBuilder("SELECT ");
        //查询语句的 from product
        StringBuilder suffix = new StringBuilder(" FROM ").append(getFromTableInSQL());
        //构建查找的条件 from product where product_id=?
        String whereCondition = buildWhereCondition(recognizer, paramAppenderList);
        if (StringUtils.isNotBlank(whereCondition)) {
            suffix.append(WHERE).append(whereCondition);
        }
        String orderBy = recognizer.getOrderBy();
        if (StringUtils.isNotBlank(orderBy)) {
            suffix.append(orderBy);
        }
        ParametersHolder parametersHolder = statementProxy instanceof ParametersHolder ? (ParametersHolder) statementProxy : null;
        String limit = recognizer.getLimit(parametersHolder, paramAppenderList);
        if (StringUtils.isNotBlank(limit)) {
            suffix.append(limit);
        }
        //拼接 from product where product_id=? for update
        suffix.append(" FOR UPDATE");
        //拼接select id,count from product where product_id=? for update
        StringJoiner selectSQLJoin = new StringJoiner(", ", prefix.toString(), suffix.toString());
        if (ONLY_CARE_UPDATE_COLUMNS) {
            //更新的字段中是否包含了主键
            if (!containsPK(updateColumns)) {
                //拼接sql select id,
                selectSQLJoin.add(getColumnNamesInSQL(tableMeta.getEscapePkNameList(getDbType())));
            }

            for (String columnName : updateColumns) {
                selectSQLJoin.add(columnName);
            }
        } else {
            for (String columnName : tableMeta.getAllColumns().keySet()) {
                selectSQLJoin.add(ColumnUtils.addEscape(columnName, getDbType()));
            }
        }
        return selectSQLJoin.toString();
    }

    @Override
    protected TableRecords afterImage(TableRecords beforeImage) throws SQLException {
        //获取到表的结构
        TableMeta tmeta = getTableMeta();
        //前置镜像没有的话，那么后置镜像也返回空
        if (beforeImage == null || beforeImage.size() == 0) {
            return TableRecords.empty(getTableMeta());
        }

        //构建后置sql的语句
        String selectSQL = buildAfterImageSQL(tmeta, beforeImage);
        ResultSet rs = null;
        try (PreparedStatement pst = statementProxy.getConnection().prepareStatement(selectSQL)) {
            SqlGenerateUtils.setParamForPk(beforeImage.pkRows(), getTableMeta().getPrimaryKeyOnlyName(), pst);
            rs = pst.executeQuery();
            return TableRecords.buildRecords(tmeta, rs);
        } finally {
            IOUtil.close(rs);
        }
    }

    private String buildAfterImageSQL(TableMeta tableMeta, TableRecords beforeImage) throws SQLException {
        // sql语句 select
        StringBuilder prefix = new StringBuilder("SELECT ");
        String whereSql = SqlGenerateUtils.buildWhereConditionByPKs(tableMeta.getPrimaryKeyOnlyName(), beforeImage.pkRows().size(), getDbType());
        // from product where product_id=?
        String suffix = " FROM " + getFromTableInSQL() + " WHERE " + whereSql;
        //select id,count from product where product_id=?
        StringJoiner selectSQLJoiner = new StringJoiner(", ", prefix.toString(), suffix);
        if (ONLY_CARE_UPDATE_COLUMNS) {
            SQLUpdateRecognizer recognizer = (SQLUpdateRecognizer) sqlRecognizer;
            //通过sql识别器去识别update更新的字段数
            List<String> updateColumns = recognizer.getUpdateColumns();
            //判断更新的的字段中是否包含主键
            if (!containsPK(updateColumns)) {
                // PK should be included. select id,
                selectSQLJoiner.add(getColumnNamesInSQL(tableMeta.getEscapePkNameList(getDbType())));
            }

            for (String columnName : updateColumns) {
                selectSQLJoiner.add(columnName);
            }
        } else {
            for (String columnName : tableMeta.getAllColumns().keySet()) {
                selectSQLJoiner.add(ColumnUtils.addEscape(columnName, getDbType()));
            }
        }
        return selectSQLJoiner.toString();
    }
}

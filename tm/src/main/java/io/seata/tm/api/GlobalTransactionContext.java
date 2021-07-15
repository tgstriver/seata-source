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
package io.seata.tm.api;

import io.seata.core.context.RootContext;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;

/**
 * GlobalTransaction API
 *
 * @author sharajava
 */
public class GlobalTransactionContext {

    private GlobalTransactionContext() {
    }

    /**
     * 创建一个全局事务，并且是发起者
     *
     * @return the new global transaction
     */
    public static GlobalTransaction createNew() {
        return new DefaultGlobalTransaction();
    }

    /**
     * 获取绑定到当前线程上的GlobalTransaction实例
     *
     * @return null if no transaction context there.
     */
    public static GlobalTransaction getCurrent() {
        // 去当前上下文中获取全局事务ID
        String xid = RootContext.getXID();
        // 没有获取到就直接返回
        if (xid == null) {
            return null;
        }

        // 获取到了，那么当前就是事务的参与者而不是发起者
        return new DefaultGlobalTransaction(xid, GlobalStatus.Begin, GlobalTransactionRole.Participant);
    }

    /**
     * 获取绑定到当前线程上的GlobalTransaction实例，如果不存在则创建一个新的
     *
     * @return new context if no existing there.
     */
    public static GlobalTransaction getCurrentOrCreate() {
        //获取当前的一个全局事务
        GlobalTransaction tx = getCurrent();
        //没有获取到全局事务,就新创建一个全局事务
        if (tx == null) {
            return createNew();
        }
        return tx;
    }

    /**
     * Reload GlobalTransaction instance according to the given XID
     *
     * @param xid the xid
     * @return reloaded transaction instance.
     * @throws TransactionException the transaction exception
     */
    public static GlobalTransaction reload(String xid) throws TransactionException {
        return new DefaultGlobalTransaction(xid, GlobalStatus.UnKnown, GlobalTransactionRole.Launcher) {
            @Override
            public void begin(int timeout, String name) throws TransactionException {
                throw new IllegalStateException("Never BEGIN on a RELOADED GlobalTransaction. ");
            }
        };
    }
}

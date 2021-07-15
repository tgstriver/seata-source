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
package io.seata.core.model;


import io.seata.common.exception.ShouldNeverHappenException;

/**
 * Status of branch transaction.
 *
 * @author sharajava
 */
public enum BranchStatus {

    /**
     * 未知的分支状态
     */
    Unknown(0),

    /**
     * 分支事务注册到了TC中
     */
    Registered(1),

    /**
     * 分支逻辑在第一阶段成功完成
     */
    PhaseOne_Done(2),

    /**
     * 分支逻辑在第一阶段失败
     */
    PhaseOne_Failed(3),

    /**
     * The Phase one timeout.
     * description:Branch logic is NOT reported for a timeout.
     */
    PhaseOne_Timeout(4),

    /**
     * 提交逻辑在第二阶段成功完成
     */
    PhaseTwo_Committed(5),

    /**
     * The Phase two commit failed retryable.
     * description:Commit logic is failed but retryable.
     */
    PhaseTwo_CommitFailed_Retryable(6),

    /**
     * The Phase two commit failed unretryable.
     * description:Commit logic is failed and NOT retryable.
     */
    PhaseTwo_CommitFailed_Unretryable(7),

    /**
     * 回滚逻辑在第二阶段成功完成
     */
    PhaseTwo_Rollbacked(8),

    /**
     * 第二阶段回滚失败可重试
     */
    PhaseTwo_RollbackFailed_Retryable(9),

    /**
     * 第二阶段回滚失败不可重试
     */
    PhaseTwo_RollbackFailed_Unretryable(10);

    private int code;

    BranchStatus(int code) {
        this.code = code;
    }

    /**
     * Gets code.
     *
     * @return the code
     */
    public int getCode() {
        return code;
    }


    /**
     * Get branch status.
     *
     * @param code the code
     * @return the branch status
     */
    public static BranchStatus get(byte code) {
        return get((int) code);
    }

    /**
     * Get branch status.
     *
     * @param code the code
     * @return the branch status
     */
    public static BranchStatus get(int code) {
        BranchStatus value = null;
        try {
            value = BranchStatus.values()[code];
        } catch (Exception e) {
            throw new ShouldNeverHappenException("Unknown BranchStatus[" + code + "]");
        }
        return value;
    }

}

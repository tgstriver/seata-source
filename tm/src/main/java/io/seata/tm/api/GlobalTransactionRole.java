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

/**
 * 当前线程在全局事务中的角色
 *
 * @author sharajava
 */
public enum GlobalTransactionRole {

    /**
     * 全局事务的发起者
     */
    Launcher,

    /**
     * 全局事务的参与者，只是加入现有的全局事务中
     */
    Participant
}

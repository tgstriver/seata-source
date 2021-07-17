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
package io.seata.server.coordinator;

import io.netty.channel.Channel;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.DurationUtil;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.context.RootContext;
import io.seata.core.event.EventBus;
import io.seata.core.event.GlobalTransactionEvent;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;
import io.seata.core.protocol.AbstractMessage;
import io.seata.core.protocol.AbstractResultMessage;
import io.seata.core.protocol.transaction.AbstractTransactionRequestToTC;
import io.seata.core.protocol.transaction.AbstractTransactionResponse;
import io.seata.core.protocol.transaction.BranchRegisterRequest;
import io.seata.core.protocol.transaction.BranchRegisterResponse;
import io.seata.core.protocol.transaction.BranchReportRequest;
import io.seata.core.protocol.transaction.BranchReportResponse;
import io.seata.core.protocol.transaction.GlobalBeginRequest;
import io.seata.core.protocol.transaction.GlobalBeginResponse;
import io.seata.core.protocol.transaction.GlobalCommitRequest;
import io.seata.core.protocol.transaction.GlobalCommitResponse;
import io.seata.core.protocol.transaction.GlobalLockQueryRequest;
import io.seata.core.protocol.transaction.GlobalLockQueryResponse;
import io.seata.core.protocol.transaction.GlobalReportRequest;
import io.seata.core.protocol.transaction.GlobalReportResponse;
import io.seata.core.protocol.transaction.GlobalRollbackRequest;
import io.seata.core.protocol.transaction.GlobalRollbackResponse;
import io.seata.core.protocol.transaction.GlobalStatusRequest;
import io.seata.core.protocol.transaction.GlobalStatusResponse;
import io.seata.core.protocol.transaction.UndoLogDeleteRequest;
import io.seata.core.rpc.Disposable;
import io.seata.core.rpc.RemotingServer;
import io.seata.core.rpc.RpcContext;
import io.seata.core.rpc.TransactionMessageHandler;
import io.seata.core.rpc.netty.ChannelManager;
import io.seata.core.rpc.netty.NettyRemotingServer;
import io.seata.server.AbstractTCInboundHandler;
import io.seata.server.event.EventBusManager;
import io.seata.server.session.GlobalSession;
import io.seata.server.session.SessionHelper;
import io.seata.server.session.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The type Default coordinator.
 */
public class DefaultCoordinator extends AbstractTCInboundHandler implements TransactionMessageHandler, Disposable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCoordinator.class);

    private static final int TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS = 5000;

    /**
     * The constant COMMITTING_RETRY_PERIOD.
     */
    protected static final long COMMITTING_RETRY_PERIOD = CONFIG.getLong(ConfigurationKeys.COMMITING_RETRY_PERIOD,
            1000L);

    /**
     * The constant ASYNC_COMMITTING_RETRY_PERIOD.
     */
    protected static final long ASYNC_COMMITTING_RETRY_PERIOD = CONFIG.getLong(
            ConfigurationKeys.ASYN_COMMITING_RETRY_PERIOD, 1000L);

    /**
     * The constant ROLLBACKING_RETRY_PERIOD.
     */
    protected static final long ROLLBACKING_RETRY_PERIOD = CONFIG.getLong(ConfigurationKeys.ROLLBACKING_RETRY_PERIOD,
            1000L);

    /**
     * The constant TIMEOUT_RETRY_PERIOD.
     */
    protected static final long TIMEOUT_RETRY_PERIOD = CONFIG.getLong(ConfigurationKeys.TIMEOUT_RETRY_PERIOD, 1000L);

    /**
     * The Transaction undo log delete period.
     */
    protected static final long UNDO_LOG_DELETE_PERIOD = CONFIG.getLong(
            ConfigurationKeys.TRANSACTION_UNDO_LOG_DELETE_PERIOD, 24 * 60 * 60 * 1000);

    /**
     * The Transaction undo log delay delete period
     */
    protected static final long UNDO_LOG_DELAY_DELETE_PERIOD = 3 * 60 * 1000;

    private static final int ALWAYS_RETRY_BOUNDARY = 0;

    private static final Duration MAX_COMMIT_RETRY_TIMEOUT = ConfigurationFactory.getInstance().getDuration(
            ConfigurationKeys.MAX_COMMIT_RETRY_TIMEOUT, DurationUtil.DEFAULT_DURATION, 100);

    /**
     * 对全局事务进行回滚时设置的最大的超时时间
     */
    private static final Duration MAX_ROLLBACK_RETRY_TIMEOUT = ConfigurationFactory.getInstance().getDuration(
            ConfigurationKeys.MAX_ROLLBACK_RETRY_TIMEOUT, DurationUtil.DEFAULT_DURATION, 100);

    private static final boolean ROLLBACK_RETRY_TIMEOUT_UNLOCK_ENABLE = ConfigurationFactory.getInstance().getBoolean(
            ConfigurationKeys.ROLLBACK_RETRY_TIMEOUT_UNLOCK_ENABLE, false);

    private ScheduledThreadPoolExecutor retryRollbacking = new ScheduledThreadPoolExecutor(1,
            new NamedThreadFactory("RetryRollbacking", 1));

    private ScheduledThreadPoolExecutor retryCommitting = new ScheduledThreadPoolExecutor(1,
            new NamedThreadFactory("RetryCommitting", 1));

    private ScheduledThreadPoolExecutor asyncCommitting = new ScheduledThreadPoolExecutor(1,
            new NamedThreadFactory("AsyncCommitting", 1));

    private ScheduledThreadPoolExecutor timeoutCheck = new ScheduledThreadPoolExecutor(1,
            new NamedThreadFactory("TxTimeoutCheck", 1));

    private ScheduledThreadPoolExecutor undoLogDelete = new ScheduledThreadPoolExecutor(1,
            new NamedThreadFactory("UndoLogDelete", 1));

    private RemotingServer remotingServer;

    private DefaultCore core;

    private EventBus eventBus = EventBusManager.get();

    /**
     * Instantiates a new Default coordinator.
     *
     * @param remotingServer the remoting server
     */
    public DefaultCoordinator(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
        this.core = new DefaultCore(remotingServer);
    }

    /**
     * 该方法用于开启全局事务
     *
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException
     */
    @Override
    protected void doGlobalBegin(GlobalBeginRequest request, GlobalBeginResponse response, RpcContext rpcContext) throws TransactionException {
        response.setXid(core.begin(rpcContext.getApplicationId(), rpcContext.getTransactionServiceGroup(),
                request.getTransactionName(), request.getTimeout()));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Begin new global transaction applicationId: {},transactionServiceGroup: {}, transactionName: {},timeout:{},xid:{}",
                    rpcContext.getApplicationId(), rpcContext.getTransactionServiceGroup(),
                    request.getTransactionName(), request.getTimeout(), response.getXid());
        }
    }

    /**
     * 该方法用于全局事务的提交
     *
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException
     */
    @Override
    protected void doGlobalCommit(GlobalCommitRequest request, GlobalCommitResponse response, RpcContext rpcContext)
            throws TransactionException {
        MDC.put(RootContext.MDC_KEY_XID, request.getXid());
        // 真正的核心业务逻辑是调用DefaultCore的commit
        response.setGlobalStatus(core.commit(request.getXid()));
    }

    /**
     * 该方法用于全局事务的回滚
     *
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException
     */
    @Override
    protected void doGlobalRollback(GlobalRollbackRequest request, GlobalRollbackResponse response,
                                    RpcContext rpcContext) throws TransactionException {
        MDC.put(RootContext.MDC_KEY_XID, request.getXid());
        response.setGlobalStatus(core.rollback(request.getXid()));
    }

    @Override
    protected void doGlobalStatus(GlobalStatusRequest request, GlobalStatusResponse response, RpcContext rpcContext)
            throws TransactionException {
        MDC.put(RootContext.MDC_KEY_XID, request.getXid());
        response.setGlobalStatus(core.getStatus(request.getXid()));
    }

    /**
     * 该方法用于全局事务的结果上报，只有saga模式才会用到
     *
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException
     */
    @Override
    protected void doGlobalReport(GlobalReportRequest request, GlobalReportResponse response, RpcContext rpcContext)
            throws TransactionException {
        MDC.put(RootContext.MDC_KEY_XID, request.getXid());
        response.setGlobalStatus(core.globalReport(request.getXid(), request.getGlobalStatus()));
    }

    /**
     * 该方法用于分支事务注册逻辑
     *
     * @param request    the request
     * @param response   the response
     * @param rpcContext the rpc context
     * @throws TransactionException
     */
    @Override
    protected void doBranchRegister(BranchRegisterRequest request, BranchRegisterResponse response,
                                    RpcContext rpcContext) throws TransactionException {
        MDC.put(RootContext.MDC_KEY_XID, request.getXid());
        // 执行的业务逻辑是DefaultCore.branchRegister()来注册分支事务
        response.setBranchId(
                core.branchRegister(request.getBranchType(), request.getResourceId(), rpcContext.getClientId(),
                        request.getXid(), request.getApplicationData(), request.getLockKey()));
    }

    /**
     * 该方法用于分支事务执行结果的上报
     *
     * @param request    the request
     * @param response
     * @param rpcContext the rpc context
     * @throws TransactionException
     */
    @Override
    protected void doBranchReport(BranchReportRequest request, BranchReportResponse response, RpcContext rpcContext)
            throws TransactionException {
        MDC.put(RootContext.MDC_KEY_XID, request.getXid());
        MDC.put(RootContext.MDC_KEY_BRANCH_ID, String.valueOf(request.getBranchId()));
        core.branchReport(request.getBranchType(), request.getXid(), request.getBranchId(), request.getStatus(), request.getApplicationData());
    }

    @Override
    protected void doLockCheck(GlobalLockQueryRequest request, GlobalLockQueryResponse response, RpcContext rpcContext)
            throws TransactionException {
        MDC.put(RootContext.MDC_KEY_XID, request.getXid());
        response.setLockable(
                core.lockQuery(request.getBranchType(), request.getResourceId(), request.getXid(), request.getLockKey()));
    }

    /**
     * 全局事务超时检查
     *
     * @throws TransactionException the transaction exception
     */
    protected void timeoutCheck() throws TransactionException {
        // 扫描global_table中的所有记录
        Collection<GlobalSession> allSessions = SessionHolder.getRootSessionManager().allSessions();
        if (CollectionUtils.isEmpty(allSessions)) {
            return;
        }

        if (allSessions.size() > 0 && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Global transaction timeout check begin, size: {}", allSessions.size());
        }

        SessionHelper.forEach(allSessions, globalSession -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        globalSession.getXid() + " " + globalSession.getStatus() + " " + globalSession.getBeginTime() + " "
                                + globalSession.getTimeout());
            }

            boolean shouldTimeout = SessionHolder.lockAndExecute(globalSession, () -> {
                if (globalSession.getStatus() != GlobalStatus.Begin || !globalSession.isTimeout()) {
                    return false;
                }

                // 全局事务的状态不是GlobalStatus.Begin并且全局事务已经超时
                globalSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
                // 关闭这个全局事务，globalSession.setActive(false);
                globalSession.close();
                // 修改global_table中的status字段为GlobalStatus.TimeoutRollbacking
                globalSession.changeStatus(GlobalStatus.TimeoutRollbacking);

                // 事务超时和开始回滚事件
                eventBus.post(new GlobalTransactionEvent(globalSession.getTransactionId(),
                        GlobalTransactionEvent.ROLE_TC,
                        globalSession.getTransactionName(),
                        globalSession.getApplicationId(),
                        globalSession.getTransactionServiceGroup(),
                        globalSession.getBeginTime(), null, globalSession.getStatus()));

                return true;
            });

            /**
             * 进入这个分支的条件包括如下情况：
             * 1. 全局事务的状态不是GlobalStatus.Begin
             * 2. 全局事务还没超时
             */
            if (!shouldTimeout) {
                // 对下一条全局事务进行处理
                return;
            }
            LOGGER.info("Global transaction[{}] is timeout and will be rollback.", globalSession.getXid());

            // 执行全局事务的回滚，由retryRollbacking异步处理
            globalSession.addSessionLifecycleListener(SessionHolder.getRetryRollbackingSessionManager());
            SessionHolder.getRetryRollbackingSessionManager().addGlobalSession(globalSession);
        });

        if (allSessions.size() > 0 && LOGGER.isDebugEnabled()) {
            LOGGER.debug("Global transaction timeout check end. ");
        }
    }

    /**
     * Handle retry rollbacking.
     */
    protected void handleRetryRollbacking() {
        /**
         * select xid,transaction_id,status,application_id,transaction_service_group,
         * transaction_name,timeout,begin_time,application_data,gmt_create,gmt_modified
         * from global_table where status in (5,4,6,7) order by gmt_modified limit 100;
         *
         * select xid,transaction_id,branch_id,resource_group_id,resource_id,branch_type,
         * status,client_id,application_data,gmt_create,gmt_modified
         * from branch_table where xid in (?,?) order by gmt_create asc;
         */
        Collection<GlobalSession> rollbackingSessions = SessionHolder.getRetryRollbackingSessionManager().allSessions();
        if (CollectionUtils.isEmpty(rollbackingSessions)) {
            return;
        }

        long now = System.currentTimeMillis();
        SessionHelper.forEach(rollbackingSessions, rollbackingSession -> {
            try {
                // 防止重复回滚
                if (rollbackingSession.getStatus().equals(GlobalStatus.Rollbacking) && !rollbackingSession.isDeadSession()) {
                    // 这里'return'等价于'continue'，继续处理下一个分支事务的回滚
                    return;
                }

                if (isRetryTimeout(now, MAX_ROLLBACK_RETRY_TIMEOUT.toMillis(), rollbackingSession.getBeginTime())) { // 全局事务回滚超时
                    // 用来控制当全局事务回滚超时的时候，是否释放全局锁，默认为false。如果设置为true，那么会释放全局锁，这样的话其他分布式事务就可以获取到全局锁
                    if (ROLLBACK_RETRY_TIMEOUT_UNLOCK_ENABLE) {
                        rollbackingSession.clean();
                    }

                    /**
                     * 防止线程安全问题
                     */
                    SessionHolder.getRetryRollbackingSessionManager().removeGlobalSession(rollbackingSession);
                    LOGGER.info("Global transaction rollback retry timeout and has removed [{}]", rollbackingSession.getXid());
                    // 这里'return'等价于'continue'，继续处理下一个分支事务的回滚
                    return;
                }

                rollbackingSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
                core.doGlobalRollback(rollbackingSession, true);
            } catch (TransactionException ex) {
                LOGGER.info("Failed to retry rollbacking [{}] {} {}", rollbackingSession.getXid(), ex.getCode(), ex.getMessage());
            }
        });
    }

    /**
     * Handle retry committing.
     */
    protected void handleRetryCommitting() {
        /**
         * select xid,transaction_id,status,application_id,transaction_service_group,
         * transaction_name,timeout,begin_time,application_data,gmt_create,gmt_modified
         * from global_table where status in (3,2) order by gmt_modified limit 100;
         *
         * select xid,transaction_id,branch_id,resource_group_id,resource_id,branch_type,
         * status,client_id,application_data,gmt_create,gmt_modified
         * from branch_table where xid in (?,?) order by gmt_create asc;
         */
        Collection<GlobalSession> committingSessions = SessionHolder.getRetryCommittingSessionManager().allSessions();
        if (CollectionUtils.isEmpty(committingSessions)) {
            return;
        }

        long now = System.currentTimeMillis();
        SessionHelper.forEach(committingSessions, committingSession -> {
            try {
                // 防止重复提交
                if (committingSession.getStatus().equals(GlobalStatus.Committing) && !committingSession.isDeadSession()) {
                    // 继续处理下一个全局事务的提交
                    return;
                }

                if (isRetryTimeout(now, MAX_COMMIT_RETRY_TIMEOUT.toMillis(), committingSession.getBeginTime())) {
                    /**
                     * 防止线程安全问题
                     */
                    SessionHolder.getRetryCommittingSessionManager().removeGlobalSession(committingSession);
                    LOGGER.error("Global transaction commit retry timeout and has removed [{}]", committingSession.getXid());
                    //The function of this 'return' is 'continue'.
                    return;
                }

                committingSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
                core.doGlobalCommit(committingSession, true);
            } catch (TransactionException ex) {
                LOGGER.info("Failed to retry committing [{}] {} {}", committingSession.getXid(), ex.getCode(), ex.getMessage());
            }
        });
    }

    private boolean isRetryTimeout(long now, long timeout, long beginTime) {
        return timeout >= ALWAYS_RETRY_BOUNDARY && now - beginTime > timeout;
    }

    /**
     * 处理全局事务的异步提交，当使用AT模式时，在seata-server服务端处理全局事务提交的时候采用的是异步处理方式
     * 实际就是在global_table中将全局事务的状态修改为了AsyncCommitting
     */
    protected void handleAsyncCommitting() {
        /**
         * select xid,transaction_id,status,application_id,transaction_service_group,
         * transaction_name,timeout,begin_time,application_data,gmt_create,gmt_modified
         * from global_table where status in (8) order by gmt_modified limit 100;
         *
         * select xid,transaction_id,branch_id,resource_group_id,resource_id,branch_type,
         * status,client_id,application_data,gmt_create,gmt_modified
         * from branch_table where xid in (?,?) order by gmt_create asc;
         */
        Collection<GlobalSession> asyncCommittingSessions = SessionHolder.getAsyncCommittingSessionManager().allSessions();
        if (CollectionUtils.isEmpty(asyncCommittingSessions)) {
            return;
        }

        SessionHelper.forEach(asyncCommittingSessions, asyncCommittingSession -> {
            try {
                // DefaultCore#asyncCommit中的指令重新排序可能会导致这种情况
                if (GlobalStatus.AsyncCommitting != asyncCommittingSession.getStatus()) {
                    return;
                }

                asyncCommittingSession.addSessionLifecycleListener(SessionHolder.getRootSessionManager());
                core.doGlobalCommit(asyncCommittingSession, true);
            } catch (TransactionException ex) {
                LOGGER.error("Failed to async committing [{}] {} {}", asyncCommittingSession.getXid(), ex.getCode(), ex.getMessage(), ex);
            }
        });
    }

    /**
     * Undo log delete.
     */
    protected void undoLogDelete() {
        Map<String, Channel> rmChannels = ChannelManager.getRmChannels();
        if (rmChannels == null || rmChannels.isEmpty()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("no active rm channels to delete undo log");
            }
            return;
        }

        // 默认undoLog日志保存7天
        short saveDays = CONFIG.getShort(ConfigurationKeys.TRANSACTION_UNDO_LOG_SAVE_DAYS, UndoLogDeleteRequest.DEFAULT_SAVE_DAYS);
        for (Map.Entry<String, Channel> channelEntry : rmChannels.entrySet()) {
            String resourceId = channelEntry.getKey();
            UndoLogDeleteRequest deleteRequest = new UndoLogDeleteRequest();
            deleteRequest.setResourceId(resourceId);
            deleteRequest.setSaveDays(saveDays > 0 ? saveDays : UndoLogDeleteRequest.DEFAULT_SAVE_DAYS);

            try {
                remotingServer.sendAsyncRequest(channelEntry.getValue(), deleteRequest);
            } catch (Exception e) {
                LOGGER.error("Failed to async delete undo log resourceId = {}, exception: {}", resourceId, e.getMessage());
            }
        }
    }

    /**
     * Init.
     */
    public void init() {
        // 定时扫描global_table中status=(5,4,6,7)的记录，对全局事务的回滚进行重试
        // 默认每隔1秒扫描一次，可以通过参数server.recovery.rollbackingRetryPeriod配置
        retryRollbacking.scheduleAtFixedRate(() -> {
            boolean lock = SessionHolder.retryRollbackingLock();
            if (lock) {
                try {
                    handleRetryRollbacking();
                } catch (Exception e) {
                    LOGGER.info("Exception retry rollbacking ... ", e);
                } finally {
                    SessionHolder.unRetryRollbackingLock();
                }
            }
        }, 0, ROLLBACKING_RETRY_PERIOD, TimeUnit.MILLISECONDS);

        retryCommitting.scheduleAtFixedRate(() -> {
            boolean lock = SessionHolder.retryCommittingLock();
            if (lock) {
                try {
                    handleRetryCommitting();
                } catch (Exception e) {
                    LOGGER.info("Exception retry committing ... ", e);
                } finally {
                    SessionHolder.unRetryCommittingLock();
                }
            }
        }, 0, COMMITTING_RETRY_PERIOD, TimeUnit.MILLISECONDS);

        // 定时扫描global_table中status=8(AsyncCommitting)的记录，进行分支事务的异步提交
        // 默认每隔1秒扫描一次，可以通过参数server.recovery.asynCommittingRetryPeriod配置
        asyncCommitting.scheduleAtFixedRate(() -> {
            boolean lock = SessionHolder.asyncCommittingLock(); // 始终返回的true
            if (lock) {
                try {
                    handleAsyncCommitting();
                } catch (Exception e) {
                    LOGGER.info("Exception async committing ... ", e);
                } finally {
                    SessionHolder.unAsyncCommittingLock();
                }
            }
        }, 0, ASYNC_COMMITTING_RETRY_PERIOD, TimeUnit.MILLISECONDS);

        // 定时扫描global_table中的所有记录，进行超时检查
        // 默认每隔1秒扫描一次，可以通过参数server.recovery.timeoutRetryPeriod配置
        timeoutCheck.scheduleAtFixedRate(() -> {
            boolean lock = SessionHolder.txTimeoutCheckLock();
            if (lock) {
                try {
                    timeoutCheck();
                } catch (Exception e) {
                    LOGGER.info("Exception timeout checking ... ", e);
                } finally {
                    SessionHolder.unTxTimeoutCheckLock();
                }
            }
        }, 0, TIMEOUT_RETRY_PERIOD, TimeUnit.MILLISECONDS);

        // 这个定时任务用于删除undoLog日志，默认延迟3分钟执行，每天执行一次
        undoLogDelete.scheduleAtFixedRate(() -> {
            boolean lock = SessionHolder.undoLogDeleteLock();
            if (lock) {
                try {
                    undoLogDelete();
                } catch (Exception e) {
                    LOGGER.info("Exception undoLog deleting ... ", e);
                } finally {
                    SessionHolder.unUndoLogDeleteLock();
                }
            }
        }, UNDO_LOG_DELAY_DELETE_PERIOD, UNDO_LOG_DELETE_PERIOD, TimeUnit.MILLISECONDS);
    }

    @Override
    public AbstractResultMessage onRequest(AbstractMessage request, RpcContext context) {
        if (!(request instanceof AbstractTransactionRequestToTC)) {
            throw new IllegalArgumentException();
        }

        AbstractTransactionRequestToTC transactionRequest = (AbstractTransactionRequestToTC) request;
        transactionRequest.setTCInboundHandler(this);

        return transactionRequest.handle(context);
    }

    @Override
    public void onResponse(AbstractResultMessage response, RpcContext context) {
        if (!(response instanceof AbstractTransactionResponse)) {
            throw new IllegalArgumentException();
        }

    }

    @Override
    public void destroy() {
        // 1. first shutdown timed task
        retryRollbacking.shutdown();
        retryCommitting.shutdown();
        asyncCommitting.shutdown();
        timeoutCheck.shutdown();
        try {
            retryRollbacking.awaitTermination(TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS, TimeUnit.MILLISECONDS);
            retryCommitting.awaitTermination(TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS, TimeUnit.MILLISECONDS);
            asyncCommitting.awaitTermination(TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS, TimeUnit.MILLISECONDS);
            timeoutCheck.awaitTermination(TIMED_TASK_SHUTDOWN_MAX_WAIT_MILLS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignore) {

        }
        // 2. second close netty flow
        if (remotingServer instanceof NettyRemotingServer) {
            ((NettyRemotingServer) remotingServer).destroy();
        }
        // 3. last destroy SessionHolder
        SessionHolder.destroy();
    }
}

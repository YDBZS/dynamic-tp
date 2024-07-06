/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dromara.dynamictp.core.executor.eager;

import org.dromara.dynamictp.common.queue.VariableLinkedBlockingQueue;
import org.springframework.lang.NonNull;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * TaskQueue in the EagerDtpExecutor。
 * Mainly used in io intensive scenario.
 *
 * @author yanhom
 * @since 1.0.3
 **/
public class TaskQueue extends VariableLinkedBlockingQueue<Runnable> {

    private static final long serialVersionUID = -1L;

    private transient EagerDtpExecutor executor;

    public TaskQueue(int queueCapacity) {
        super(queueCapacity);
    }

    public void setExecutor(EagerDtpExecutor exec) {
        executor = exec;
    }

    @Override
    public boolean offer(@NonNull Runnable runnable) {
        if (executor == null) {
            throw new RejectedExecutionException("The task queue does not have executor.");
        }
        int currentPoolThreadSize = executor.getPoolSize();
        // 线程池中的线程数等于最大线程数的时候，就将任务放进队列等待工作线程处理
        if (currentPoolThreadSize == executor.getMaximumPoolSize()) {
            return super.offer(runnable);
        }
        // have free worker. put task into queue to let the worker deal with task.
        // 如果当前未执行的任务数量小于等于当前线程数，还有剩余的Worker的进程，就将任务放进队列等待工作线程处理。
        if (executor.getSubmittedTaskCount() < currentPoolThreadSize) {
            return super.offer(runnable);
        }
        // return false to let executor create new worker.
        // 如果当前线程数大于核心线程，但小于最大线程数，则直接返回false，外层逻辑线程池创建新的线程来执行任务
        if (currentPoolThreadSize < executor.getMaximumPoolSize()) {
            return false;
        }
        // currentPoolThreadSize >= max
        return super.offer(runnable);
    }

    /**
     * Force offer task
     *
     * @param o task
     * @param timeout timeout
     * @param unit unit
     * @return offer success or not
     * @throws RejectedExecutionException if executor is terminated.
     * @throws InterruptedException if interrupted while waiting.
     */
    public boolean force(Runnable o, long timeout, TimeUnit unit) throws InterruptedException {
        if (executor.isShutdown()) {
            throw new RejectedExecutionException("Executor is shutdown.");
        }
        return super.offer(o, timeout, unit);
    }
}

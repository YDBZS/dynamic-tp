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

package org.dromara.dynamictp.core.spring;

import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.collections4.CollectionUtils;
import org.dromara.dynamictp.common.entity.DtpExecutorProps;
import org.dromara.dynamictp.common.properties.DtpProperties;
import org.dromara.dynamictp.common.spring.SpringBeanHelper;
import org.dromara.dynamictp.core.executor.ExecutorType;
import org.dromara.dynamictp.core.executor.NamedThreadFactory;
import org.dromara.dynamictp.core.executor.eager.EagerDtpExecutor;
import org.dromara.dynamictp.core.executor.eager.TaskQueue;
import org.dromara.dynamictp.core.executor.priority.PriorityDtpExecutor;
import org.dromara.dynamictp.core.reject.RejectHandlerGetter;
import org.dromara.dynamictp.core.support.BinderHelper;
import org.dromara.dynamictp.core.support.task.wrapper.TaskWrappers;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotationMetadata;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import static org.dromara.dynamictp.common.constant.DynamicTpConst.ALLOW_CORE_THREAD_TIMEOUT;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.AWAIT_TERMINATION_SECONDS;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.AWARE_NAMES;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.NOTIFY_ENABLED;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.NOTIFY_ITEMS;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.PLATFORM_IDS;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.PLUGIN_NAMES;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.PRE_START_ALL_CORE_THREADS;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.QUEUE_TIMEOUT;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.REJECT_ENHANCED;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.REJECT_HANDLER_TYPE;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.RUN_TIMEOUT;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.TASK_WRAPPERS;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.THREAD_POOL_ALIAS_NAME;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.THREAD_POOL_NAME;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.TRY_INTERRUPT_WHEN_TIMEOUT;
import static org.dromara.dynamictp.common.constant.DynamicTpConst.WAIT_FOR_TASKS_TO_COMPLETE_ON_SHUTDOWN;
import static org.dromara.dynamictp.common.em.QueueTypeEnum.buildLbq;
import static org.dromara.dynamictp.common.entity.NotifyItem.mergeAllNotifyItems;

/**
 * DtpBeanDefinitionRegistrar related
 * DtpBeanDefinitionRegistar实现了ConfigurationClassPostProcessor，利用Spring
 * 的动态注册bean机制，在bean初始化之前注册 BeanDefinition 以达到注入bean的目的
 * <p>
 * 最终被Spring ConfigurationClassPostProcessor 执行出来，
 *
 * @author yanhom
 * @since 1.0.4
 **/
@Slf4j
public class DtpBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar, EnvironmentAware {

    private Environment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    /**
     * registerBeanDefinitions方法中主要做了这么几件事
     * 1、从Environment 读取配置信息绑定到 DtpProperties
     * 2、获取配置文件中绑定的线程池，如果没有则结束
     * 3、遍历线程池，绑定配置构造线程池所需要的属性，根据配置中的executorType注册注册不同类型的线程池Bean
     * 4、BeanUtil#registerIfAbsent()注册Bean
     * <p>
     * ExecutorType目前支持3种类型，分别对应三个线程池。
     *
     * @param importingClassMetadata annotation metadata of the importing
     *                               class  导入类的注册元数据
     * @param registry               current bean definition registry
     *                               当前bean的定义注册器
     */
    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata,
                                        BeanDefinitionRegistry registry) {
        DtpProperties dtpProperties = DtpProperties.getInstance();
        // 从Enviroment读取配置信息绑定到 DtpProperties
        BinderHelper.bindDtpProperties(environment, dtpProperties);
        // 获取配置文件中配置的线程池
        val executors = dtpProperties.getExecutors();
        if (CollectionUtils.isEmpty(executors)) {
            log.warn("DynamicTp registrar, no executors are configured.");
            return;
        }

        // 遍历并注册线程池 BeanDefinitiion
        executors.forEach(e -> {
            // 类型选择，common -> DtpExecutor, eager -> EagerDtpExecutor
            Class<?> executorTypeClass = ExecutorType.getClass(e.getExecutorType());
            // 通过ThreadPoolProperties 来构造线程池所需要的属性
            Map<String, Object> propertyValues = buildPropertyValues(e);
            Object[] args = buildConstructorArgs(executorTypeClass, e);
            //  工具类 BeanDefinition注册 Bean 相当于手动用@Bean声明线程池对象
            SpringBeanHelper.register(registry, e.getThreadPoolName(), executorTypeClass, propertyValues, args);
        });
    }

    /**
     * 通过ThreadPoolProperties来构造线程池所需要的属性
     *
     * @param props 线程池的属性
     * @return java.util.Map<java.lang.String, java.lang.Object>
     * @author debao.yang
     * @since 2024/7/5 19:32
     */
    private Map<String, Object> buildPropertyValues(DtpExecutorProps props) {
        Map<String, Object> propertyValues = Maps.newHashMap();
        propertyValues.put(THREAD_POOL_NAME, props.getThreadPoolName());
        propertyValues.put(THREAD_POOL_ALIAS_NAME, props.getThreadPoolAliasName());
        propertyValues.put(ALLOW_CORE_THREAD_TIMEOUT, props.isAllowCoreThreadTimeOut());
        propertyValues.put(WAIT_FOR_TASKS_TO_COMPLETE_ON_SHUTDOWN, props.isWaitForTasksToCompleteOnShutdown());
        propertyValues.put(AWAIT_TERMINATION_SECONDS, props.getAwaitTerminationSeconds());
        propertyValues.put(PRE_START_ALL_CORE_THREADS, props.isPreStartAllCoreThreads());
        propertyValues.put(REJECT_HANDLER_TYPE, props.getRejectedHandlerType());
        propertyValues.put(REJECT_ENHANCED, props.isRejectEnhanced());
        propertyValues.put(RUN_TIMEOUT, props.getRunTimeout());
        propertyValues.put(TRY_INTERRUPT_WHEN_TIMEOUT, props.isTryInterrupt());
        propertyValues.put(QUEUE_TIMEOUT, props.getQueueTimeout());
        val notifyItems = mergeAllNotifyItems(props.getNotifyItems());
        propertyValues.put(NOTIFY_ITEMS, notifyItems);
        propertyValues.put(PLATFORM_IDS, props.getPlatformIds());
        propertyValues.put(NOTIFY_ENABLED, props.isNotifyEnabled());

        val taskWrappers = TaskWrappers.getInstance()
                .getByNames(props.getTaskWrapperNames());
        propertyValues.put(TASK_WRAPPERS, taskWrappers);
        propertyValues.put(PLUGIN_NAMES, props.getPluginNames());
        propertyValues.put(AWARE_NAMES, props.getAwareNames());
        return propertyValues;
    }

    /**
     * 选择阻塞队列，这里针对EagerDtpExecutor做了单独处理，选择了TaskQueue作为阻塞队列
     *
     * @param clazz 执行的线程池类型
     * @param props 线程池的参数
     * @return java.lang.Object[]
     * @author debao.yang
     * @since 2024/7/5 19:34
     */
    private Object[] buildConstructorArgs(Class<?> clazz,
                                          DtpExecutorProps props) {
        BlockingQueue<Runnable> taskQueue;
        // 如果是 EagerDtpExecutor 的话，对工作队列就是 TaskQueue
        if (clazz.equals(EagerDtpExecutor.class)) {
            taskQueue = new TaskQueue(props.getQueueCapacity());
        } else if (clazz.equals(PriorityDtpExecutor.class)) {
            // 如果是优先线程池执行器的话，工作队列就是 PriorityBlockingQueue
            taskQueue = new PriorityBlockingQueue<>(props.getQueueCapacity(), PriorityDtpExecutor.getRunnableComparator());
        } else {
            // 以上两种都不是的话，就根据配置中的 queryType 来选择阻塞的队列
            taskQueue = buildLbq(props.getQueueType(),
                    props.getQueueCapacity(),
                    props.isFair(),
                    props.getMaxFreeMemory());
        }

        return new Object[]{
                props.getCorePoolSize(),
                props.getMaximumPoolSize(),
                props.getKeepAliveTime(),
                props.getUnit(),
                taskQueue,
                new NamedThreadFactory(props.getThreadNamePrefix()),
                RejectHandlerGetter.buildRejectedHandler(props.getRejectedHandlerType())
        };
    }

}

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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.dromara.dynamictp.common.plugin.DtpInterceptorRegistry;
import org.dromara.dynamictp.common.util.ConstructorUtil;
import org.dromara.dynamictp.common.util.ReflectionUtil;
import org.dromara.dynamictp.core.DtpRegistry;
import org.dromara.dynamictp.core.executor.DtpExecutor;
import org.dromara.dynamictp.core.executor.eager.EagerDtpExecutor;
import org.dromara.dynamictp.core.executor.eager.TaskQueue;
import org.dromara.dynamictp.core.support.DynamicTp;
import org.dromara.dynamictp.core.support.ExecutorWrapper;
import org.dromara.dynamictp.core.support.ScheduledThreadPoolExecutorProxy;
import org.dromara.dynamictp.core.support.ThreadPoolExecutorProxy;
import org.dromara.dynamictp.core.support.task.wrapper.TaskWrapper;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.core.task.TaskDecorator;
import org.springframework.core.type.MethodMetadata;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

import static org.dromara.dynamictp.core.support.DtpLifecycleSupport.shutdownGracefulAsync;

/**
 * DtpPostProcessor 利用了Spring容器启动BeanPostProcessor机制增强机制，在Bean
 * 初始化的时候调用postProcessAfterInitialization，它实现了获取被IOC容器托管的线程池bean然后注册到本地的注册表中
 * <p>
 * BeanPostProcessor that handles all related beans managed by Spring.
 *
 * @author yanhom
 * @since 1.0.0
 **/
@Slf4j
@SuppressWarnings("all")
public class DtpPostProcessor implements BeanPostProcessor, BeanFactoryAware, PriorityOrdered {

    private static final String REGISTER_SOURCE = "beanPostProcessor";

    private DefaultListableBeanFactory beanFactory;

    /**
     * Compatible with lower versions of Spring.
     *
     * @param bean     the new bean instance
     * @param beanName the name of the bean
     * @return the bean instance to use
     * @throws BeansException in case of errors
     */
    @Override
    public Object postProcessBeforeInitialization(Object bean,
                                                  String beanName) throws BeansException {
        return bean;
    }

    /**
     * 1、获取到bean后，如果是非线程池类型则结束。
     * 2、如果是DtpExecutor则注册到DTP_REGISTRY注册表中(一个ConcurrentHashMap)
     * 3、如果是 非动态线程池 且标注了 @DynamicTp 注解则注册到 COMMON_REGISTRY 注册表中
     * 4、如果是 非动态线程池 且未标注 @DynamicTp 注解则结束，不做增强
     *
     * @param bean the new bean instance
     * @param beanName the name of the bean
     * @return
     * @throws BeansException
     */
    @Override
    public Object postProcessAfterInitialization(@NonNull Object bean,
                                                 @NonNull String beanName) throws BeansException {
        // 只增强线程池相关的类
        if (!(bean instanceof ThreadPoolExecutor) && !(bean instanceof ThreadPoolTaskExecutor)) {
            return bean;
        }

        // 如果是 DtpExecutor 类型注册到注册表 
        if (bean instanceof DtpExecutor) {
            return registerAndReturnDtp(bean);
        }
        // register juc ThreadPoolExecutor or ThreadPoolTaskExecutor
        return registerAndReturnCommon(bean, beanName);
    }

    /**
     * 如果是DtpExecutor类型注册到注册表 DTP_REGISTRY
     *
     * @param bean 动态线程池的实例
     * @return java.lang.Object
     * @author debao.yang
     * @since 2024/7/5 20:03
     */
    private Object registerAndReturnDtp(Object bean) {
        DtpExecutor dtpExecutor = (DtpExecutor) bean;
        Object[] args = ConstructorUtil.buildTpExecutorConstructorArgs(dtpExecutor);
        Class<?>[] argTypes = ConstructorUtil.buildTpExecutorConstructorArgTypes();
        Set<String> pluginNames = dtpExecutor.getPluginNames();

        val enhancedBean = (DtpExecutor) DtpInterceptorRegistry.plugin(bean, pluginNames, argTypes, args);
        if (enhancedBean instanceof EagerDtpExecutor) {
            ((TaskQueue) enhancedBean.getQueue()).setExecutor((EagerDtpExecutor) enhancedBean);
        }
        DtpRegistry.registerExecutor(ExecutorWrapper.of(enhancedBean), REGISTER_SOURCE);
        return enhancedBean;
    }

    private Object registerAndReturnCommon(Object bean, String beanName) {
        String dtpAnnoValue;
        try {
            // 获取标注 @DynamicTp 注解的bean，则为基本线程池，但受到组件管理监控
            DynamicTp dynamicTp = beanFactory.findAnnotationOnBean(beanName, DynamicTp.class);
            if (Objects.nonNull(dynamicTp)) {
                dtpAnnoValue = dynamicTp.value();
            } else {
                BeanDefinition beanDefinition = beanFactory.getBeanDefinition(beanName);
                if (!(beanDefinition instanceof AnnotatedBeanDefinition)) {
                    return bean;
                }
                AnnotatedBeanDefinition annotatedBeanDefinition = (AnnotatedBeanDefinition) beanDefinition;
                MethodMetadata methodMetadata = (MethodMetadata) annotatedBeanDefinition.getSource();
                if (Objects.isNull(methodMetadata) || !methodMetadata.isAnnotated(DynamicTp.class.getName())) {
                    return bean;
                }
                dtpAnnoValue = Optional.ofNullable(methodMetadata.getAnnotationAttributes(DynamicTp.class.getName()))
                        .orElse(Collections.emptyMap())
                        .getOrDefault("value", "")
                        .toString();
            }
        } catch (NoSuchBeanDefinitionException e) {
            log.warn("There is no bean with the given name {}", beanName, e);
            return bean;
        }
        String poolName = StringUtils.isNotBlank(dtpAnnoValue) ? dtpAnnoValue : beanName;
        return doRegisterAndReturnCommon(bean, poolName);
    }

    private Object doRegisterAndReturnCommon(Object bean, String poolName) {
        if (bean instanceof ThreadPoolTaskExecutor) {
            ThreadPoolTaskExecutor poolTaskExecutor = (ThreadPoolTaskExecutor) bean;
            val proxy = newProxy(poolName, poolTaskExecutor.getThreadPoolExecutor());
            try {
                ReflectionUtil.setFieldValue("threadPoolExecutor", bean, proxy);
                tryWrapTaskDecorator(poolTaskExecutor, proxy);
            } catch (IllegalAccessException ignored) {
            }
            DtpRegistry.registerExecutor(new ExecutorWrapper(poolName, proxy), REGISTER_SOURCE);
            return bean;
        }
        Executor proxy;
        if (bean instanceof ScheduledThreadPoolExecutor) {
            proxy = newScheduledTpProxy(poolName, (ScheduledThreadPoolExecutor) bean);
        } else {
            proxy = newProxy(poolName, (ThreadPoolExecutor) bean);
        }
        DtpRegistry.registerExecutor(new ExecutorWrapper(poolName, proxy), REGISTER_SOURCE);
        return proxy;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = (DefaultListableBeanFactory) beanFactory;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    private ThreadPoolExecutorProxy newProxy(String name,
                                             ThreadPoolExecutor originExecutor) {
        val proxy = new ThreadPoolExecutorProxy(originExecutor);
        shutdownGracefulAsync(originExecutor, name, 0);
        return proxy;
    }

    private ScheduledThreadPoolExecutorProxy newScheduledTpProxy(String name,
                                                                 ScheduledThreadPoolExecutor originExecutor) {
        val proxy = new ScheduledThreadPoolExecutorProxy(originExecutor);
        shutdownGracefulAsync(originExecutor, name, 0);
        return proxy;
    }

    private void tryWrapTaskDecorator(ThreadPoolTaskExecutor poolTaskExecutor,
                                      ThreadPoolExecutorProxy proxy) throws IllegalAccessException {
        Object taskDecorator = ReflectionUtil.getFieldValue("taskDecorator", poolTaskExecutor);
        if (Objects.isNull(taskDecorator)) {
            return;
        }
        TaskWrapper taskWrapper = (taskDecorator instanceof TaskWrapper) ? (TaskWrapper) taskDecorator : new TaskWrapper() {
            @Override
            public String name() {
                return taskDecorator.getClass().getName();
            }

            @Override
            public Runnable wrap(Runnable runnable) {
                return ((TaskDecorator) taskDecorator).decorate(runnable);
            }
        };
        ReflectionUtil.setFieldValue("taskWrappers", proxy, Lists.newArrayList(taskWrapper));
    }
}

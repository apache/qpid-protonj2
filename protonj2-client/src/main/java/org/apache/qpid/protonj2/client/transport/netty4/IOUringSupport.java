/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.qpid.protonj2.client.transport.netty4;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.ThreadFactory;

import org.apache.qpid.protonj2.client.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

@SuppressWarnings("unchecked")
public final class IOUringSupport {

    private static final Logger LOG = LoggerFactory.getLogger(IOUringSupport.class);

    public static final String NAME = "IO_URING";

    private static final boolean AVAILABLE;
    private static final boolean INCUBATOR_VARIANT;
    private static final Class<? extends Channel> SOCKET_CHANNEL_CLASS;
    private static final Constructor<?> EVENTLOOP_CONSTRUCTOR;
    private static final Method IOHANDLER_FACTORY;

    static {
        boolean available = false;
        boolean incubator = false;
        Class<? extends Channel> socketChannelClass = null;
        Constructor<?> constructor = null;
        Method ioHandlerFactory = null;

        // Try for new Netty built in IoUring before falling back to incubator checks
        try {
            final Class<?> ioUring = Class.forName("io.netty.channel.uring.IoUring");
            final Method isAvailable = ioUring.getDeclaredMethod("isAvailable", (Class<?>[])null);
            final Class<?> eventLoopGroup = Class.forName("io.netty.channel.MultiThreadIoEventLoopGroup");
            final Class<?> ioUringHandler = Class.forName("io.netty.channel.uring.IoUringIoHandler");
            final Class<?> ioUringHandlerFactory = Class.forName("io.netty.channel.IoHandlerFactory");

            constructor = eventLoopGroup.getDeclaredConstructor(int.class, ThreadFactory.class, ioUringHandlerFactory);
            ioHandlerFactory = ioUringHandler.getDeclaredMethod("newFactory");
            socketChannelClass = (Class<? extends Channel>) Class.forName("io.netty.channel.uring.IoUringSocketChannel");
            available = (boolean) isAvailable.invoke(null);
        } catch (Exception e) {
            LOG.debug("Unable to enable netty io_uring support due to error", e);
        }

        if (!available) {
            try {
                final Class<?> ioUring = Class.forName("io.netty.incubator.channel.uring.IOUring");
                final Method isAvailable = ioUring.getDeclaredMethod("isAvailable");
                final Class<?> eventLoopGroup = Class.forName("io.netty.incubator.channel.uring.IOUringEventLoopGroup");

                socketChannelClass = (Class<? extends Channel>) Class.forName("io.netty.incubator.channel.uring.IOUringSocketChannel");
                constructor = eventLoopGroup.getDeclaredConstructor(int.class, ThreadFactory.class);
                available = (boolean) isAvailable.invoke(null);
                incubator = true;
            } catch (Exception e) {
                LOG.debug("Unable to enable netty incubator io_uring support due to error", e);
            }
        }

        AVAILABLE = available;
        INCUBATOR_VARIANT = incubator;
        SOCKET_CHANNEL_CLASS = socketChannelClass;
        EVENTLOOP_CONSTRUCTOR = constructor;
        IOHANDLER_FACTORY = ioHandlerFactory;
    }

    public static boolean isAvailable(TransportOptions transportOptions) {
        return transportOptions.allowNativeIO() && AVAILABLE;
    }

    public static boolean isAvailable() {
        return AVAILABLE;
    }

    public static EventLoopGroup createGroup(int nThreads, ThreadFactory ioThreadFactory) {
        ensureAvailability();

        Exception createError = null;

        if (INCUBATOR_VARIANT) {
            try {
                return (EventLoopGroup) EVENTLOOP_CONSTRUCTOR.newInstance(nThreads, ioThreadFactory);
            } catch (Exception e) {
                LOG.debug("Unable to create Netty incubator io_uring EventLoopGroup due to error", e);
                createError = e;
            }
        } else {
            try {
                return (EventLoopGroup) EVENTLOOP_CONSTRUCTOR.newInstance(nThreads, ioThreadFactory, IOHANDLER_FACTORY.invoke(null));
            } catch (Exception e) {
                LOG.debug("Unable to create Netty io_uring EventLoopGroup due to error", e);
                createError = e;
            }
        }

        throw (Error) new UnsupportedOperationException("Netty io_uring failed to create resource").initCause(createError);
    }

    public static Class<? extends Channel> getChannelClass() {
        ensureAvailability();

        return SOCKET_CHANNEL_CLASS;
    }

    public static void ensureAvailability() {
        if (!AVAILABLE) {
            throw new UnsupportedOperationException(
                "Netty io_ring support is not enabled because the Netty library indicates it is not present or disabled");
        }
    }
}

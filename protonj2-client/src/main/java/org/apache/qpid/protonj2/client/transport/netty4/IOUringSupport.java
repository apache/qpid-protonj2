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

import java.util.concurrent.ThreadFactory;

import org.apache.qpid.protonj2.client.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringSocketChannel;

public final class IOUringSupport {

    private static final Logger LOG = LoggerFactory.getLogger(IOUringSupport.class);

    public static final String NAME = "IO_URING";

    public static boolean isAvailable(TransportOptions transportOptions) {
        return transportOptions.allowNativeIO() && isAvailable();
    }

    public static boolean isAvailable() {
        try {
            return IoUring.isAvailable();
        } catch (UnsatisfiedLinkError | Exception e) {
            LOG.debug("Unable to enable netty io_uring support due to error", e);
            return false;
        }
    }

    public static EventLoopGroup createGroup(int nThreads, ThreadFactory ioThreadFactory) {
        ensureAvailability();

        return new MultiThreadIoEventLoopGroup(nThreads, ioThreadFactory, IoUringIoHandler.newFactory());
    }

    public static Class<? extends Channel> getChannelClass() {
        ensureAvailability();

        return IoUringSocketChannel.class;
    }

    public static void ensureAvailability() {
        if (!isAvailable()) {
            throw new UnsupportedOperationException(
                "Netty io_ring support is not enabled because the Netty library indicates it is not present or disabled");
        }
    }
}

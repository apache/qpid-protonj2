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
package org.apache.qpid.protonj2.client.transport;

import java.util.concurrent.ThreadFactory;

import org.apache.qpid.protonj2.client.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;

public class KQueueSupport {

    private static final Logger LOG = LoggerFactory.getLogger(KQueueSupport.class);

    public static final String NAME = "KQUEUE";

    public static boolean isAvailable(TransportOptions transportOptions) {
        try {
            return transportOptions.allowNativeIO() && KQueue.isAvailable();
        } catch (NoClassDefFoundError ncdfe) {
            LOG.debug("Unable to check for KQueue support due to missing class definition", ncdfe);
            return false;
        }
    }

    public static EventLoopGroup createGroup(int nThreads, ThreadFactory ioThreadFactory) {
        return new KQueueEventLoopGroup(nThreads, ioThreadFactory);
    }

    public static Class<? extends Channel> getChannelClass() {
        return KQueueSocketChannel.class;
    }
}

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
package org.apache.qpid.protonj2.client.transport.netty5;

import java.util.concurrent.ThreadFactory;

import org.apache.qpid.protonj2.client.TransportOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty5.channel.Channel;
import io.netty5.channel.EventLoopGroup;

public class IOUringSupport {

    private static final Logger LOG = LoggerFactory.getLogger(IOUringSupport.class);

    public static final String NAME = "IO_URING";

    public static boolean isAvailable(TransportOptions transportOptions) {
        try {
            return false; //transportOptions.allowNativeIO() && IOUring.isAvailable();
        } catch (NoClassDefFoundError ncdfe) {
            LOG.debug("Unable to check for IO_Uring support due to missing class definition", ncdfe);
            return false;
        }
    }

    public static EventLoopGroup createGroup(int nThreads, ThreadFactory ioThreadFactory) {
        // return new MultithreadEventLoopGroup(nThreads, ioThreadFactory, null);
        throw new UnsupportedOperationException("Netty v5 doesn't yet support IO_Uring");
    }

    public static Class<? extends Channel> getChannelClass() {
        // return IOUringSocketChannel.class;
        throw new UnsupportedOperationException("Netty v5 doesn't yet support IO_Uring");
    }
}

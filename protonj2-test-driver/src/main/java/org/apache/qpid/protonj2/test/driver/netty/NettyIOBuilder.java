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

package org.apache.qpid.protonj2.test.driver.netty;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import org.apache.qpid.protonj2.test.driver.ProtonTestClientOptions;
import org.apache.qpid.protonj2.test.driver.ProtonTestServerOptions;
import org.apache.qpid.protonj2.test.driver.netty.netty4.Netty4Client;
import org.apache.qpid.protonj2.test.driver.netty.netty4.Netty4Server;
import org.apache.qpid.protonj2.test.driver.netty.netty4.Netty4Support;
import org.apache.qpid.protonj2.test.driver.netty.netty5.Netty5Client;
import org.apache.qpid.protonj2.test.driver.netty.netty5.Netty5Server;
import org.apache.qpid.protonj2.test.driver.netty.netty5.Netty5Support;

/**
 * An I/O context used to abstract the implementation of the IO layer in use.
 */
public interface NettyIOBuilder {

    /**
     * Create an NettyClient from the available options.
     *
     * @param options
     * 		The {@link ProtonTestClientOptions} that configure the IO Transport the context creates.
     * @param connectedHandler
     * 		A handler that should be invoked when a connection attempt succeeds.
     * @param inputHandler
     * 		A {@link Consumer} that accept incoming {@link ByteBuffer} data from the remote.
     *
     * @return a new {@link NettyClient} from available options.
     */
    public static NettyClient createClient(ProtonTestClientOptions options, Runnable connectedHandler, Consumer<ByteBuffer> inputHandler) {
        if (Netty4Support.isAvailable()) {
            return new Netty4Client(options, connectedHandler, inputHandler);
        } else if (Netty5Support.isAvailable()) {
            return new Netty5Client(options, connectedHandler, inputHandler);
        }

        throw new UnsupportedOperationException("Netty not available on the class path");
    }

    /**
     * Create an NettyServer from the available options.
     *
     * @param options
     * 		The {@link ProtonTestServerOptions} that configure the IO Transport the context creates.
     * @param connectedHandler
     * 		A handler that should be invoked when a connection attempt succeeds.
     * @param inputHandler
     * 		A {@link Consumer} that accept incoming {@link ByteBuffer} data from the remote.
     *
     * @return a new {@link NettyServer} from available options.
     */
    public static NettyServer createServer(ProtonTestServerOptions options, Runnable connectedHandler, Consumer<ByteBuffer> inputHandler) {
        if (Netty4Support.isAvailable()) {
            return new Netty4Server(options, connectedHandler, inputHandler);
        } else if (Netty5Support.isAvailable()) {
            return new Netty5Server(options, connectedHandler, inputHandler);
        }

        throw new UnsupportedOperationException("Netty not available on the class path");
    }
}
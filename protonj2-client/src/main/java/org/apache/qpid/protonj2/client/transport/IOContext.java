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

import org.apache.qpid.protonj2.client.SslOptions;
import org.apache.qpid.protonj2.client.TransportOptions;
import org.apache.qpid.protonj2.client.transport.netty4.Netty4IOContext;
import org.apache.qpid.protonj2.client.transport.netty4.Netty4Support;
import org.apache.qpid.protonj2.client.transport.netty5.Netty5IOContext;
import org.apache.qpid.protonj2.client.transport.netty5.Netty5Support;
import org.apache.qpid.protonj2.engine.Scheduler;

/**
 * An I/O context used to abstract the implementation of the IO layer in use.
 */
public interface IOContext {

    /**
     * Shutdown the event loop synchronously with a grace period for work that might be in-bound
     * at the time of termination.  This is not safe to call from inside the event loop as it blocks
     * until the shutdown has completed.
     */
    void shutdown();

    /**
     * Shutdown the event loop asynchronously with a grace period for work that might be in-bound
     * at the time of termination.  This is safe to call from inside the event loop where the
     * standard blocking shutdown API is not.
     */
    void shutdownAsync();

    /**
     * @return the single threaded IO work scheduler for this {@link IOContext}
     */
    Scheduler ioScheduler();

    /**
     * @return a new {@link Transport} instance that is tied to this {@link IOContext}
     */
    Transport newTransport();

    /**
     * Create an IOContext from the available options.
     *
     * @param options
     * 		The {@link TransportOptions} that configure the IO Transport the context creates.
     * @param sslOptions
     * 		The {@link SslOptions} that configure the SSL layer of the IO Transport the context creates.
     * @param ioThreadName
     * 		The name to given the single IO Thread the context must provide.
     *
     * @return a new {@link IOContext} from available options.
     */
    static IOContext create(TransportOptions options, SslOptions sslOptions, String ioThreadName) {
        if (Netty4Support.isAvailable()) {
            return new Netty4IOContext(options, sslOptions, ioThreadName);
        } else if (Netty5Support.isAvailable()) {
            return new Netty5IOContext(options, sslOptions, ioThreadName);
        }

        throw new UnsupportedOperationException("Netty not available on the class path");
    }
}
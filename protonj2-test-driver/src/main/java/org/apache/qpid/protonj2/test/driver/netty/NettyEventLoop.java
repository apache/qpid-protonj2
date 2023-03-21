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

import java.util.concurrent.TimeUnit;

/**
 * Abstraction around Netty event loops to allow change of Netty version
 */
public interface NettyEventLoop {

    /**
     * @return true if the current thread is the event loop thread.
     */
    boolean inEventLoop();

    /**
     * Run the given {@link Runnable} on the Netty event loop.
     *
     * @param runnable
     * 		The {@link Runnable} to schedule to run on the event loop.
     */
    void execute(Runnable runnable);

    /**
     * Schedule the given {@link Runnable} to run on the event loop after the given delay
     *
     * @param runnable
     * 		The {@link Runnable} to schedule to run on the event loop.
     * @param delay
     * 		The time delay before the {@link Runnable} should be executed.
     * @param unit
     * 		The units that the time delay is given in.
     */
    void schedule(Runnable runnable, int delay, TimeUnit unit);

}

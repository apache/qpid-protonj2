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

package org.apache.qpid.protonj2.test.driver.netty.netty4;

import java.util.concurrent.TimeUnit;

import org.apache.qpid.protonj2.test.driver.netty.NettyEventLoop;

import io.netty.channel.EventLoop;

public final class Netty4EventLoop implements NettyEventLoop {

    private final EventLoop loop;

    public Netty4EventLoop(EventLoop loop) {
        this.loop = loop;
    }

    @Override
    public boolean inEventLoop() {
        return loop.inEventLoop();
    }

    @Override
    public void execute(Runnable runnable) {
        loop.execute(runnable);
    }

    @Override
    public void schedule(Runnable runnable, int delay, TimeUnit unit) {
        loop.schedule(runnable, delay, unit);
    }
}

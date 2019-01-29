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
package org.apache.qpid.proton4j.engine.impl;

import org.apache.qpid.proton4j.buffer.ProtonBufferAllocator;
import org.apache.qpid.proton4j.buffer.ProtonByteBufferAllocator;
import org.apache.qpid.proton4j.engine.EngineConfiguration;

/**
 * Proton engine configuration API
 */
public class ProtonEngineConfiguration implements EngineConfiguration {

    private int saslMaxFrameSize;
    private int maxFrameSize;
    private ProtonBufferAllocator allocator = ProtonByteBufferAllocator.DEFAULT;

    private int remoteMaxFrameSize;

    @Override
    public int getSaslMaxFrameSize() {
        return saslMaxFrameSize;
    }

    public void setSaslMaxFrameSize(int maxFrameSize) {
        this.saslMaxFrameSize = maxFrameSize;
    }

    @Override
    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    public void setMaxFrameSize(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    public int getRemoteMaxFrameSize() {
        return remoteMaxFrameSize;
    }

    public void setRemoteMaxFrameSize(int remoteMaxFrameSize) {
        this.remoteMaxFrameSize = remoteMaxFrameSize;
    }

    public int getOutboundMaxFrameSize() {
        return getMaxFrameSize(); // TODO
    }

    @Override
    public ProtonBufferAllocator getBufferAllocator() {
        return allocator;
    }

    @Override
    public void setBufferAllocator(ProtonBufferAllocator allocator) {
        this.allocator = allocator;
    }
}

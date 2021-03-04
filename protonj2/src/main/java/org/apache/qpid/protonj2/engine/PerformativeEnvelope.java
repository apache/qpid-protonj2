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
package org.apache.qpid.protonj2.engine;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;

/**
 * Base class for envelope types that travel through the engine.
 *
 * @param <V> The type of body that this {@link PerformativeEnvelope} will carry.
 */
public abstract class PerformativeEnvelope<V> {

    private final byte frameType;

    private V body;
    private int channel;
    private ProtonBuffer payload;

    protected PerformativeEnvelope(byte frameType) {
        this.frameType = frameType;
    }

    PerformativeEnvelope<V> initialize(V body, int channel, ProtonBuffer payload) {
        this.body = body;
        this.channel = channel;
        this.payload = payload;

        return this;
    }

    /**
     * @return the decoded body of the performative that this envelope carries..
     */
    public V getBody() {
        return body;
    }

    /**
     * @return the channel that the wrapped body and payload was sent on
     */
    public int getChannel() {
        return channel;
    }

    /**
     * @return the frame type that is assigned to this envelope
     */
    public byte getFrameType() {
        return frameType;
    }

    /**
     * @return the binary payload that was delivered with this envelope
     */
    public ProtonBuffer getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "PerformativeEnvelope:[" + body + ", " + channel + ", " + ", " + payload + "]";
    }
}
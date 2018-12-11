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
package org.apache.qpid.proton4j.transport;

import org.apache.qpid.proton4j.amqp.Binary;

/**
 * Base class for Frames that travel through the Transport
 */
public abstract class Frame<V> {

    private V body;
    private short channel;
    private byte type;
    private Binary payload;

    public Frame(V body, short channel, byte type, Binary payload) {
        this.body = body;
        this.channel = channel;
        this.type = type;
        this.payload = payload;
    }

    public V getBody() {
        return body;
    }

    public short getChannel() {
        return channel;
    }

    public byte getType() {
        return type;
    }

    public Binary getPayload() {
        return payload;
    }

    /**
     * Used to release a Frame that was taken from a Frame pool in order
     * to make it available for the next input operations.  Once called the
     * contents of the Frame are invalid and cannot be used again inside the
     * same context.
     */
    public void release() {
        body = null;
        payload = null;
        channel = -1;
        type = -1;

        // TODO
    }
}
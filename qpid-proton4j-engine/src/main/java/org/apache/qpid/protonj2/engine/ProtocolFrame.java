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

import org.apache.qpid.protonj2.types.transport.Performative;
import org.apache.qpid.protonj2.types.transport.Performative.PerformativeHandler;

/**
 * Frame object that carries an AMQP Performative
 */
public class ProtocolFrame extends Frame<Performative> {

    public static final byte AMQP_FRAME_TYPE = (byte) 0;

    private ProtocolFramePool pool;

    ProtocolFrame() {
        this(null);
    }

    ProtocolFrame(ProtocolFramePool pool) {
        super(AMQP_FRAME_TYPE);

        this.pool = pool;
    }

    /**
     * Used to release a Frame that was taken from a Frame pool in order
     * to make it available for the next input operations.  Once called the
     * contents of the Frame are invalid and cannot be used again inside the
     * same context.
     */
    public void release() {
        initialize(null, -1, -1, null);

        if (pool != null) {
            pool.release(this);
        }
    }

    public <E> void invoke(PerformativeHandler<E> handler, E context) {
        getBody().invoke(handler, getPayload(), getChannel(), context);
    }
}

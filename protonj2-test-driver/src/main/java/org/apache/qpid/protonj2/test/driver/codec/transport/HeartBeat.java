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
package org.apache.qpid.protonj2.test.driver.codec.transport;

import io.netty.buffer.ByteBuf;

/**
 * Dummy Performative that is fired whenever an Empty frame is received
 */
public class HeartBeat extends PerformativeDescribedType {

    public static final HeartBeat INSTANCE = new HeartBeat();

    private HeartBeat() {
        super(0);
    }

    @Override
    public Object getDescriptor() {
        return null;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.HEARTBEAT;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, int frameSize, ByteBuf payload, int channel, E context) {
        handler.handleHeartBeat(frameSize, INSTANCE, payload, channel, context);
    }
}

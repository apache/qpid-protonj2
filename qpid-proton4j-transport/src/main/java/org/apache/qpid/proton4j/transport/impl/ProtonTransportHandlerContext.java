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
package org.apache.qpid.proton4j.transport.impl;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.transport.PartialFrame;
import org.apache.qpid.proton4j.transport.ProtocolFrame;
import org.apache.qpid.proton4j.transport.SaslFrame;
import org.apache.qpid.proton4j.transport.TransportHandlerContext;

/**
 * Context for a registered TransportHandler
 */
public class ProtonTransportHandlerContext implements TransportHandlerContext {

    ProtonTransportHandlerContext previous;
    ProtonTransportHandlerContext next;

    @Override
    public void fireRead(ProtonBuffer buffer) {
    }

    @Override
    public void fireAMQPHeader(AMQPHeader header) {
    }

    @Override
    public void fireSaslFrame(SaslFrame frame) {
    }

    @Override
    public void fireProtocolFrame(ProtocolFrame frame) {
    }

    @Override
    public void firePartialFrame(PartialFrame frame) {
    }

    @Override
    public void fireEncodingError(Throwable e) {
    }

    @Override
    public void fireDecodingError(Throwable e) {
    }

    @Override
    public void fireFailed(Throwable e) {
    }

    @Override
    public void fireWrite(ProtocolFrame frame) {
    }

    @Override
    public void fireWrite(SaslFrame frame) {
    }

    @Override
    public void fireWrite(ProtonBuffer buffer) {
    }

    @Override
    public void fireFlush() {
    }
}

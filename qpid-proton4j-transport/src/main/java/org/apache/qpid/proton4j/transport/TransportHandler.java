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

import org.apache.qpid.proton4j.amqp.security.SaslPerformative;
import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;
import org.apache.qpid.proton4j.amqp.transport.Performative;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Listen for events generated from the Transport
 */
public interface TransportHandler {

    // Life cycle events for a handler

    void handlerAdded(TransportHandlerContext context) throws Exception;

    void handlerRemoved(TransportHandlerContext context) throws Exception;

    // Some things that might flow through a transport pipeline

    // Read events

    void handleReadabilityChanged(TransportHandlerContext context);  // TODO how to communicate readable state ?

    void handleRead(TransportHandlerContext context, ProtonBuffer buffer);

    void handleRead(TransportHandlerContext context, HeaderFrame header);

    void handleRead(TransportHandlerContext context, SaslFrame frame);

    void handleRead(TransportHandlerContext context, ProtocolFrame frame);

    // Write events

    void handleWritabilityChanged(TransportHandlerContext context); // TODO how to communicate writable state ?

    void handleWrite(TransportHandlerContext context, AMQPHeader header);

    void handleWrite(TransportHandlerContext context, Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge);

    void handleWrite(TransportHandlerContext context, SaslPerformative performative);

    // TODO - The Frame<?> type is a little confusing here in that it carries both the body and the payload
    //        along with some channel and type info.  We could instead provide inbound and outbond frame types
    //        to allow for distinct APIs on each.  Or specify that the write of a ProtonBuffer is frame scoped
    //        such that this variant isn't needed.
    void handleWrite(TransportHandlerContext context, Frame<ProtonBuffer> frame);

    // TODO - Can replace above with variants for outbound versions of the inbound Frame types.
    //        the handler that encodes can then handle writes of all and signal their handler on
    //        frame to large conditions etc.

    // void handleWrite(TransportHandlerContext context, OutboundHeaderFrame header);
    // void handleWrite(TransportHandlerContext context, OutboundProtocolFrame frame);
    // void handleWrite(TransportHandlerContext context, OutboundSaslFrame frame);

    void handleWrite(TransportHandlerContext context, ProtonBuffer buffer);

    // Error events

    void transportEncodingError(TransportHandlerContext context, Throwable e);

    void transportDecodingError(TransportHandlerContext context, Throwable e);

    void transportFailed(TransportHandlerContext context, Throwable e);
}

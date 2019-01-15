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
 * Pipeline of handlers for Transport work.
 */
public interface TransportPipeline {

    Transport getTransport();

    //----- Pipeline management ----------------------------------------------//

    TransportPipeline addFirst(String name, TransportHandler handler);

    TransportPipeline addLast(String name, TransportHandler handler);

    TransportPipeline removeFirst();

    TransportPipeline removeLast();

    TransportPipeline remove(String name);

    TransportHandler first();

    TransportHandler last();

    TransportHandlerContext firstContext();

    TransportHandlerContext lastContext();

    //----- Event triggers ---------------------------------------------------//

    TransportPipeline fireRead(ProtonBuffer input);

    TransportPipeline fireRead(HeaderFrame header);

    TransportPipeline fireRead(SaslFrame frame);

    TransportPipeline fireRead(ProtocolFrame frame);

    TransportPipeline fireWrite(ProtonBuffer buffer);

    TransportPipeline fireWrite(AMQPHeader header);

    TransportPipeline fireWrite(Performative performative, short channel, ProtonBuffer payload, Runnable payloadToLarge);

    TransportPipeline fireWrite(SaslPerformative performative);

    TransportPipeline fireEncodingError(Throwable e);

    TransportPipeline fireDecodingError(Throwable e);

    TransportPipeline fireFailed(Throwable e);

}

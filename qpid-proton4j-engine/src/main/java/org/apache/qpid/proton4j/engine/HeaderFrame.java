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
package org.apache.qpid.proton4j.engine;

import org.apache.qpid.proton4j.amqp.transport.AMQPHeader;

/**
 * Frame type that carries AMQPHeader instances
 */
public class HeaderFrame extends Frame<AMQPHeader> {

    public static final byte HEADER_FRAME_TYPE = (byte) 1;

    public static final HeaderFrame SASL_HEADER_FRAME = new HeaderFrame(AMQPHeader.getSASLHeader());

    public static final HeaderFrame AMQP_HEADER_FRAME = new HeaderFrame(AMQPHeader.getRawAMQPHeader());

    public HeaderFrame(AMQPHeader body) {
        super(HEADER_FRAME_TYPE);

        initialize(body, 0, null);
    }
}

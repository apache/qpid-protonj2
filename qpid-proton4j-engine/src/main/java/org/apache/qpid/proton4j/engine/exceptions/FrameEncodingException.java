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
package org.apache.qpid.proton4j.engine.exceptions;

import org.apache.qpid.proton4j.amqp.transport.AmqpError;

/**
 * Exception thrown when the engine cannot encode a frame from a given performative
 * and or payload combination.
 */
public class FrameEncodingException extends ProtocolViolationException {

    private static final long serialVersionUID = -5392939106677054003L;

    public FrameEncodingException() {
        super(AmqpError.INTERNAL_ERROR);
    }

    public FrameEncodingException(String message, Throwable cause) {
        super(AmqpError.INTERNAL_ERROR, message, cause);
    }

    public FrameEncodingException(String message) {
        super(AmqpError.INTERNAL_ERROR, message);
    }

    public FrameEncodingException(Throwable cause) {
        super(AmqpError.INTERNAL_ERROR, cause);
    }
}

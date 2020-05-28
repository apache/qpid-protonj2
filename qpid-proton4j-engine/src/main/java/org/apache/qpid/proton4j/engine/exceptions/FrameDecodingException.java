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
 * Exception thrown when the engine cannot decode an incoming frame due to some
 * error either with the encoding itself or the contents which cause a specification
 * violation.
 */
public class FrameDecodingException extends ProtocolViolationException {

    private static final long serialVersionUID = -1226121804157774724L;

    public FrameDecodingException() {
        super(AmqpError.DECODE_ERROR);
    }

    public FrameDecodingException(String message, Throwable cause) {
        super(AmqpError.DECODE_ERROR, message, cause);
    }

    public FrameDecodingException(String message) {
        super(AmqpError.DECODE_ERROR, message);
    }

    public FrameDecodingException(Throwable cause) {
        super(AmqpError.DECODE_ERROR, cause);
    }
}

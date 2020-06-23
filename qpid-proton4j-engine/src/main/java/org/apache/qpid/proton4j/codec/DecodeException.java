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
package org.apache.qpid.proton4j.codec;

/**
 * Exception thrown when a type decoder fails due to some unrecoverable error or
 * violation of AMQP type specifications for an incoming encoded byte sequence.
 */
public class DecodeException extends IllegalArgumentException {

    private static final long serialVersionUID = 5579043130487516118L;

    /**
     * Creates a generic {@link DecodeException} with no cause or error description set.
     */
    public DecodeException() {
    }

    /**
     * Creates a {@link DecodeException} with the given error message.
     *
     * @param message
     *      The error message to convey with the exception.
     */
    public DecodeException(String message) {
        super(message);
    }

    /**
     * Creates a {@link DecodeException} with the given assigned root cause exception.
     *
     * @param cause
     *      The underlying exception that triggered this encode error to be thrown.
     */
    public DecodeException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a {@link DecodeException} with the given error message and assigned
     * root cause exception.
     *
     * @param message
     *      The error message to convey with the exception.
     * @param cause
     *      The underlying exception that triggered this encode error to be thrown.
     */
    public DecodeException(String message, Throwable cause) {
        super(message, cause);
    }
}

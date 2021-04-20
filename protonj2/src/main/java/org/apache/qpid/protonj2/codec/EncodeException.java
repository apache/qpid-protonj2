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
package org.apache.qpid.protonj2.codec;

/**
 * Exception thrown when a type encoder fails due to some unrecoverable error or
 * violation of AMQP type specifications.
 */
public class EncodeException extends IllegalArgumentException {

    private static final long serialVersionUID = 4909721739062393272L;

    /**
     * Creates a generic {@link EncodeException} with no cause or error description set.
     */
    public EncodeException() {
    }

    /**
     * Creates a {@link EncodeException} with the given error message.
     *
     * @param message
     *      The error message to convey with the exception.
     */
    public EncodeException(String message) {
        super(message);
    }

    /**
     * Creates a {@link EncodeException} with the given assigned root cause exception.
     *
     * @param cause
     *      The underlying exception that triggered this encode error to be thrown.
     */
    public EncodeException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a {@link EncodeException} with the given error message and assigned
     * root cause exception.
     *
     * @param message
     *      The error message to convey with the exception.
     * @param cause
     *      The underlying exception that triggered this encode error to be thrown.
     */
    public EncodeException(String message, Throwable cause) {
        super(message, cause);
    }
}

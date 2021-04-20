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
package org.apache.qpid.protonj2.client.exceptions;

import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ErrorCondition;
import org.apache.qpid.protonj2.types.transport.Close;

/**
 * Exception thrown when the remote peer actively closes the {@link Connection} by sending
 * and AMQP {@link Close} frame or when the IO layer is disconnected due to some other
 * reason such as a security error or transient network error.
 */
public class ClientConnectionRemotelyClosedException extends ClientIOException {

    private static final long serialVersionUID = 5728349272688210550L;

    private final ErrorCondition condition;

    public ClientConnectionRemotelyClosedException(String message) {
        this(message, (ErrorCondition) null);
    }

    public ClientConnectionRemotelyClosedException(String message, Throwable cause) {
        this(message, cause, null);
    }

    public ClientConnectionRemotelyClosedException(String message, ErrorCondition condition) {
        super(message);
        this.condition = condition;
    }

    public ClientConnectionRemotelyClosedException(String message, Throwable cause, ErrorCondition condition) {
        super(message, cause);
        this.condition = condition;
    }

    /**
     * @return the {@link ErrorCondition} that was provided by the remote to describe the cause of the close.
     */
    public ErrorCondition getErrorCondition() {
        return condition;
    }
}

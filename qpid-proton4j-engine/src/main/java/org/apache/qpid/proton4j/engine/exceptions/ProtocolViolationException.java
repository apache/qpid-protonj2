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

import org.apache.qpid.proton4j.types.Symbol;

/**
 * Error thrown when there has been a violation of the AMQP specification
 */
public class ProtocolViolationException extends ProtonException {

    private static final long serialVersionUID = 1L;

    private Symbol condition;

    public ProtocolViolationException() {
        super();
    }

    public ProtocolViolationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProtocolViolationException(String message) {
        super(message);
    }

    public ProtocolViolationException(Throwable cause) {
        super(cause);
    }

    public ProtocolViolationException(Symbol condition) {
        super();

        this.condition = condition;
    }

    public ProtocolViolationException(Symbol condition, String message, Throwable cause) {
        super(message, cause);

        this.condition = condition;
    }

    public ProtocolViolationException(Symbol condition, String message) {
        super(message);

        this.condition = condition;
    }

    public ProtocolViolationException(Symbol condition, Throwable cause) {
        super(cause);

        this.condition = condition;
    }

    public Symbol getErrorCondition() {
        return condition;
    }
}

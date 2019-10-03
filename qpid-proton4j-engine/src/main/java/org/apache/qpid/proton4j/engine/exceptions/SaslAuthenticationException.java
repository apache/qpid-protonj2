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

import org.apache.qpid.proton4j.amqp.security.SaslCode;

/**
 * Indicates a failure in the SASL negotiation that resulted in a filed outcome.
 *
 * The result code of the SASL outcome is provided and can be used to determine if
 * the error was temporary or a permanent error.
 */
public class SaslAuthenticationException extends SaslException {

    private static final long serialVersionUID = 8764127837435090076L;

    private final SaslCode outcome;

    public SaslAuthenticationException(SaslCode outcome) {
        this(outcome, null, null);
    }

    public SaslAuthenticationException(SaslCode outcome, String message, Throwable cause) {
        super(message, cause);

        this.outcome = outcome;
    }

    public SaslAuthenticationException(SaslCode outcome, String message) {
        this(outcome, message, null);
    }

    public SaslAuthenticationException(SaslCode outcome, Throwable cause) {
        this(outcome, null, cause);
    }

    public SaslCode getOutcome() {
        return outcome;
    }
}

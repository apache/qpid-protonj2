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

import javax.security.sasl.SaslException;

/**
 * Indicates an error occurred during SASL Authentication and the engine has now
 * transition to a failed state.
 */
public class EngineSaslException extends EngineFailedException {

    private static final long serialVersionUID = 5614580652746664144L;

    public EngineSaslException(String message, SaslException cause) {
        super(message, cause);
    }

    public EngineSaslException(SaslException cause) {
        super(cause);
    }

    @Override
    public SaslException getCause() {
        return (SaslException) super.getCause();
    }
}

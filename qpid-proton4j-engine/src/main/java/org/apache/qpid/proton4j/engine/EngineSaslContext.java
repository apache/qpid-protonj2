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

import org.apache.qpid.proton4j.engine.sasl.SaslConstants.SaslOutcomes;

/**
 * Context for the Engine that exposes SASL state and configuration.
 * <p>
 * When configured for SASL authentication the SASL context provides a view of the
 * current state of the authentication and allows for configuration of the SASL layer
 * prior to the start of the authentication process.  Once authentication is complete
 * the context provides a means of determining the outcome of process.
 *
 * TODO - One possible way to allow for SASL state to be visible at the engine level
 */
public interface EngineSaslContext {

    public enum SaslState {

        /**
         * Engine not started, context can be configured
         */
        IDLE,

        /**
         * Engine started and set configuration in use
         */
        AUTHENTICATING,

        /**
         * Authentication succeeded
         */
        AUTHENTICATED,

        /**
         * Authentication failed
         */
        AUTHENTICATION_FAILED,

        /**
         * Authentication skipped when allowed by configuration
         */
        SKIPPED,

        /**
         * No authentication layer configured.
         */
        DISABLED

    }

    /**
     * @return the current state of SASL Authentication.
     */
    SaslState getSaslState();

    // TODO - Provide some information on the authenticated result (user etc)

    /**
     * Provides a low level outcome value for the SASL authentication process.
     *
     * @return the SASL outcome code that results from authentication
     */
    SaslOutcomes getSaslOutcome();

    //----- Configuration

    /**
     * @return the currently configured max frame size allowed for SASL frames.
     */
    int getMaxFrameSize();

    /**
     * Set the maximum frame size the remote can send before an error is indicated.
     *
     * @param maxFrameSize
     *      The maximum allowed frame size from the remote sender.
     */
    void setMaxFrameSize(int maxFrameSize);

}

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
package org.apache.qpid.protonj2.engine;

import org.apache.qpid.protonj2.engine.sasl.SaslClientContext;
import org.apache.qpid.protonj2.engine.sasl.SaslOutcome;
import org.apache.qpid.protonj2.engine.sasl.SaslServerContext;

/**
 * Driver for the Engine that exposes SASL state and configuration.
 * <p>
 * When configured for SASL authentication the SASL driver provides a view of the
 * current state of the authentication and allows for configuration of the SASL layer
 * prior to the start of the authentication process.  Once authentication is complete
 * the driver provides a means of determining the outcome of process.
 */
public interface EngineSaslDriver {

	/**
	 * The SASL driver state used to determine at what point the current SASL negotiation process
	 * is currently in.  If the state is 'none' then no SASL negotiations will be performed.
	 */
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
         * No authentication layer configured.
         */
        NONE

    }

    /**
     * Configure this {@link EngineSaslDriver} to operate in client mode and return the associated
     * {@link SaslClientContext} instance that should be used to complete the SASL negotiation
     * with the server end.
     *
     * @return the SASL client context.
     *
     * @throws IllegalStateException if the engine is already in server mode or the engine has not
     *                               been configure with SASL support.
     */
    SaslClientContext client();

    /**
     * Configure this {@link EngineSaslDriver} to operate in server mode and return the associated
     * {@link SaslServerContext} instance that should be used to complete the SASL negotiation
     * with the client end.
     *
     * @return the SASL server context.
     *
     * @throws IllegalStateException if the engine is already in client mode or the engine has not
     *                               been configure with SASL support.
     */
    SaslServerContext server();

    /**
     * Returns a SaslState that indicates the current operating state of the SASL
     * negotiation process or conversely if no SASL layer is configured this method
     * should return the disabled state.  This method must never return a null result.
     *
     * @return the current state of SASL Authentication.
     */
    SaslState getSaslState();

    /**
     * Provides a low level outcome value for the SASL authentication process.
     * <p>
     * If the SASL exchange is ongoing or the SASL layer was skipped because a
     * particular engine configuration allows such behavior then this method
     * should return null to indicate no SASL outcome is available.
     *
     * @return the SASL outcome code that results from authentication
     */
    SaslOutcome getSaslOutcome();

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

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
package org.apache.qpid.proton4j.engine.impl.sasl;

import org.apache.qpid.proton4j.engine.EngineSaslDriver;
import org.apache.qpid.proton4j.engine.impl.sasl.SaslConstants.SaslOutcomes;
import org.apache.qpid.proton4j.engine.impl.sasl.SaslConstants.SaslStates;
import org.apache.qpid.proton4j.engine.sasl.SaslClientContext;
import org.apache.qpid.proton4j.engine.sasl.SaslOutcome;
import org.apache.qpid.proton4j.engine.sasl.SaslServerContext;

/**
 * Proton4J Engine SASL Context implementation.
 */
public class ProtonEngineSaslDriver implements EngineSaslDriver {

    /**
     * Default max frame size value used by this engine SASL context if not otherwise configured.
     */
    public final static int MIN_MAX_SASL_FRAME_SIZE = 4096;

    private SaslState saslState = SaslState.IDLE;
    private SaslOutcome saslOutcome;
    private int maxFrameSize = MIN_MAX_SASL_FRAME_SIZE;
    private ProtonSaslContext context;

    @Override
    public SaslClientContext client() {
        if (context != null && context.isServer()) {
            throw new IllegalStateException("Engine SASL Context already operating in server mode");
        }

        if (context == null) {
            // TODO
        }

        return (SaslClientContext) context;
    }

    @Override
    public SaslServerContext server() {
        if (context != null && context.isClient()) {
            throw new IllegalStateException("Engine SASL Context already operating in client mode");
        }

        if (context == null) {
            // TODO
        }

        return (SaslServerContext) context;
    }

    @Override
    public SaslState getSaslState() {
        return saslState;
    }

    @Override
    public SaslOutcome getSaslOutcome() {
        return saslOutcome;
    }

    @Override
    public int getMaxFrameSize() {
        return maxFrameSize;
    }

    @Override
    public void setMaxFrameSize(int maxFrameSize) {
        if (saslState == SaslState.IDLE) {
            this.maxFrameSize = maxFrameSize;
        } else {
            throw new IllegalStateException("Cannot configure max SASL frame size after SASL negotiations have started");
        }
    }

    //----- Internal Engine SASL Context API

    protected SaslStates classifyStateFromOutcome(SaslOutcomes outcome) {
        return outcome == SaslOutcomes.SASL_OK ? SaslStates.SASL_PASS : SaslStates.SASL_FAIL;
    }
}

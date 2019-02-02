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
package org.apache.qpid.proton4j.engine.impl;

import org.apache.qpid.proton4j.engine.EngineSaslContext;
import org.apache.qpid.proton4j.engine.sasl.SaslConstants;
import org.apache.qpid.proton4j.engine.sasl.SaslConstants.SaslOutcomes;

/**
 * A Default No-Op SASL context that is used to provide the engine with a stub
 * when no SASL is configured for the operating engine.
 */
public class ProtonEngineNoOpSaslContext implements EngineSaslContext {

    @Override
    public SaslState getSaslState() {
        return SaslState.DISABLED;
    }

    @Override
    public SaslOutcomes getSaslOutcome() {
        return SaslOutcomes.PN_SASL_SKIPPED;
    }

    @Override
    public int getMaxFrameSize() {
        return SaslConstants.MAX_SASL_FRAME_SIZE;
    }

    @Override
    public void setMaxFrameSize(int maxFrameSize) {
    }

    @Override
    public void setAutnenticateOnStart(boolean authOnStart) {
    }

    @Override
    public boolean isAuthenticateOnStart() {
        return false;
    }
}

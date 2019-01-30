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

import org.apache.qpid.proton4j.engine.Connection;
import org.apache.qpid.proton4j.engine.Engine;
import org.apache.qpid.proton4j.engine.EngineListener;
import org.apache.qpid.proton4j.engine.EnginePipeline;
import org.apache.qpid.proton4j.engine.EngineSaslContext;

/**
 * The default Proton-J Transport implementation.
 */
public class ProtonEngine implements Engine {

    private final ProtonEnginePipeline pipeline;

    private ProtonEngineConfiguration configuration;
    private EngineListener listener;

    public ProtonEngine() {
        this.pipeline = new ProtonEnginePipeline(this);
    }

    @Override
    public Connection start() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub
    }

    //----- Transport configuration ------------------------------------------//

    @Override
    public EnginePipeline getPipeline() {
        return pipeline;
    }

    @Override
    public void setEngineListener(EngineListener listener) {
        this.listener = listener;
    }

    @Override
    public EngineListener getEngineListener() {
        return listener;
    }

    @Override
    public ProtonEngineConfiguration getConfiguration() {
        return configuration;
    }

    @Override
    public EngineSaslContext getSaslContext() {
        // TODO
        return null;
    }
}

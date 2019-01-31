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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Listener for events from a Proton Engine implementation.
 */
public interface EngineListener {

    /**
     * Called when the Engine has encountered an unrecoverable error
     * and is now in a failed (closed) state an cannot process any more work.
     *
     * @param engine
     *      The Engine that has failed
     * @param cause
     *      The error that indicates the reason for the failure.
     */
    void engineFailed(Engine engine, Throwable cause);

    /**
     * Provides data from the engine to be written to some external source
     *
     * TODO - Do we want to have the engine inform of writes or require a handler from
     *        the client to handle this always.
     */
    void handleEngineOutput(Engine engine, ProtonBuffer output);

}

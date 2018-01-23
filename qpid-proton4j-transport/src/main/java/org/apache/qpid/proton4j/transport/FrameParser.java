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
package org.apache.qpid.proton4j.transport;

import java.io.IOException;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Interface for an Frame parser object.
 */
public interface FrameParser {

    /**
     * Used to reset the parser back to an initial state or flush resources
     * following completion of some processing stage.
     */
    void reset();

    /**
     * Parse the incoming data and provide events to the parent Transport
     * based on the contents of that data.
     *
     * @param context
     *      The TransportHandlerContext that applies to the current event
     * @param incoming
     *      The ProtonBuffer containing new data to be parsed.
     *
     * @throws IOException if an error occurs while parsing incoming data.
     */
    void parse(TransportHandlerContext context, ProtonBuffer incoming) throws IOException;

}

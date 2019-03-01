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
package org.apache.qpid.proton4j.engine.test;

import org.apache.qpid.proton4j.amqp.Binary;

/**
 * Test driver object used to drive inputs and inspect outputs of an Engine.
 */
public class EngineTestDriver {

    /**
     * Create a test driver instance connected to the given Engine instance.
     *
     * @param engine
     *      the engine to be tested.
     */
    public EngineTestDriver() {
    }

    /**
     * @param error
     *      The error that describes why the assertion failed.
     */
    public void assertionFailed(AssertionError error) {
        // TODO ?
    }

    /**
     * @param _type
     * @param _channel
     * @param _frameDescribedType
     * @param _framePayload
     * @param _deferWrite
     * @param _sendDelay
     */
    public void sendFrame(FrameType _type, int _channel, ListDescribedType _frameDescribedType, Binary _framePayload, boolean _deferWrite, long _sendDelay) {
        // TODO Auto-generated method stub

    }

    /**
     * @param _response
     */
    public void sendHeader(byte[] _response) {
        // TODO Auto-generated method stub

    }
}

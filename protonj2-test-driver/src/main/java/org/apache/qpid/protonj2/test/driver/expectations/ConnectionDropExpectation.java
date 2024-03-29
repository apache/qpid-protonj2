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

package org.apache.qpid.protonj2.test.driver.expectations;

import org.apache.qpid.protonj2.test.driver.ScriptedExpectation;

/**
 * Expectation used to script an expected connection drop from the remotely
 * connected peer.
 * <p>
 * This expectation type is best used to await a remote peer response of
 * dropping the connection in relation to some scripted action that will
 * result in it closing its side of the connection.
 */
public class ConnectionDropExpectation implements ScriptedExpectation {

    /**
     * Creates a simple connection dropped expectation instance.
     */
    public ConnectionDropExpectation() {
        // No setup needed as the driver should handle this explicitly.
    }
}

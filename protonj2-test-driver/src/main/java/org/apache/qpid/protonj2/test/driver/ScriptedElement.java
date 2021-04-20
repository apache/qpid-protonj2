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
package org.apache.qpid.protonj2.test.driver;

import org.apache.qpid.protonj2.test.driver.codec.security.SaslDescribedType;
import org.apache.qpid.protonj2.test.driver.codec.transport.AMQPHeader;
import org.apache.qpid.protonj2.test.driver.codec.transport.PerformativeDescribedType;

/**
 * Root of scripted entries in the test driver script.
 */
public interface ScriptedElement extends AMQPHeader.HeaderHandler<AMQPTestDriver>,
                                         PerformativeDescribedType.PerformativeHandler<AMQPTestDriver>,
                                         SaslDescribedType.SaslPerformativeHandler<AMQPTestDriver> {

    enum ScriptEntryType {
        EXPECTATION,
        ACTION
    }

    /**
     * @return the type of script entry this instance represents
     */
    ScriptEntryType getType();

    /**
     * @return true if this element represents an optional part of the script.
     */
    default boolean isOptional() {
        return false;
    }

    /**
     * @return a {@link ScriptedAction} to perform after the element has been performed.
     */
    default ScriptedAction performAfterwards() {
        return null;
    }
}

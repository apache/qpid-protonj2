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
package org.apache.qpid.protonj2.client;

import java.util.Map;

import org.apache.qpid.protonj2.client.impl.ClientErrorCondition;

/**
 * Conveys the error value used to inform the user of why an endpoint
 * was closed or a delivery rejected.
 */
public interface ErrorCondition {

    /**
     * @return a value that indicates the type of error condition.
     */
    String condition();

    /**
     * Descriptive text that supplies any supplementary details not indicated by the condition value..
     *
     * @return supplementary details not indicated by the condition value..
     */
    String description();

    /**
     * @return a {@link Map} carrying information about the error condition.
     */
    Map<String, Object> info();

    //----- Factory Methods for Default Error Condition types

    /**
     * Create an error condition object using the supplied values.  The condition string
     * cannot be null however the other attribute can.
     *
     * @param condition
     * 		The value that defines the error condition.
     * @param description
     * 		The supplementary description for the given error condition.
     * @param info
     * 		A {@link Map} containing additional error information.
     *
     * @return a new read-only {@link ErrorCondition} object
     */
    static ErrorCondition create(String condition, String description, Map<String, Object> info) {
        return new ClientErrorCondition(condition, description, info);
    }
}

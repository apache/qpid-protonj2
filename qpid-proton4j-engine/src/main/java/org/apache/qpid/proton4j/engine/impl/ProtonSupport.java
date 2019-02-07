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

import org.apache.qpid.proton4j.amqp.transport.ErrorCondition;
import org.apache.qpid.proton4j.engine.AsyncResult;
import org.apache.qpid.proton4j.engine.exceptions.ProtonException;
import org.apache.qpid.proton4j.engine.util.FailedResult;
import org.apache.qpid.proton4j.engine.util.SucceededResult;

/**
 * Utility methods used in the proton engine
 */
public final class ProtonSupport {

    private ProtonSupport() {
    }

    static <T> AsyncResult<T> result(T value, ErrorCondition error) {
        if (error != null && error.getCondition() != null) {
            return new FailedResult<>(new ProtonException(error.toString()));
        } else {
            return new SucceededResult<T>(value);
        }
    }
}

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
package org.apache.qpid.proton4j.engine.test.expectations;

import org.apache.qpid.proton4j.amqp.transport.Detach;
import org.hamcrest.Matcher;

/**
 * Scripted expectation for the AMQP Detach performative
 */
public class DetachExpectation extends AbstractExceptation<Detach> {

    /**
     * Enumeration which maps to fields in the Detach Performative
     */
    public enum Field {
        HANDLE,
        CLOSED,
        ERROR
    }

    public DetachExpectation withHandle(Matcher<?> m) {
        getMatchers().put(Field.HANDLE, m);
        return this;
    }

    public DetachExpectation withClosed(Matcher<?> m) {
        getMatchers().put(Field.CLOSED, m);
        return this;
    }

    public DetachExpectation withError(Matcher<?> m) {
        getMatchers().put(Field.ERROR, m);
        return this;
    }

    @Override
    protected Object getFieldValue(Detach end, Enum<?> performativeField) {
        Object result = null;

        if (performativeField == Field.HANDLE) {
            result = end.hasHandle() ? end.getHandle() : null;
        } else if (performativeField == Field.CLOSED) {
            result = end.hasClosed() ? end.getClosed() : null;
        } else if (performativeField == Field.ERROR) {
            result = end.hasError() ? end.getError() : null;
        } else {
            throw new AssertionError("Request for unknown field in type Detach");
        }

        return result;
    }

    @Override
    protected Enum<?> getFieldEnum(int fieldIndex) {
        return Field.values()[fieldIndex];
    }

    @Override
    protected Class<Detach> getExpectedTypeClass() {
        return Detach.class;
    }
}

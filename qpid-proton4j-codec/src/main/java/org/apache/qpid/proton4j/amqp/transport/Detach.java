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
package org.apache.qpid.proton4j.amqp.transport;

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;

public final class Detach implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000016L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:detach:list");

    private UnsignedInteger handle;
    private boolean closed;
    private ErrorCondition error;

    public UnsignedInteger getHandle() {
        return handle;
    }

    public void setHandle(UnsignedInteger handle) {
        if (handle == null) {
            throw new NullPointerException("the handle field is mandatory");
        }

        this.handle = handle;
    }

    public boolean getClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    public ErrorCondition getError() {
        return error;
    }

    public void setError(ErrorCondition error) {
        this.error = error;
    }

    @Override
    public Detach copy() {
        Detach copy = new Detach();

        copy.setHandle(handle);
        copy.setClosed(closed);
        copy.setError(error == null ? null : error.copy());

        return copy;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.DETACH;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, Binary payload, E context) {
        handler.handleDetach(this, payload, context);
    }

    @Override
    public String toString() {
        return "Detach{" +
               "handle=" + handle +
               ", closed=" + closed +
               ", error=" + error +
               '}';
    }
}

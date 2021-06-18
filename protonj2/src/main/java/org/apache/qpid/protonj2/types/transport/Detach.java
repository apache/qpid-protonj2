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
package org.apache.qpid.protonj2.types.transport;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.Symbol;
import org.apache.qpid.protonj2.types.UnsignedLong;

public final class Detach implements Performative {

    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000016L);
    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:detach:list");

    private static final long UINT_MAX = 0xFFFFFFFFL;

    private static final int HANDLE = 1;
    private static final int CLOSED = 2;
    private static final int ERROR = 4;

    private int modified = 0;

    // TODO - Consider using the matching signed types instead of next largest
    //        for these values as in most cases we don't actually care about sign.
    //        In the cases we do care we could just do the math and make these
    //        interfaces simpler and not check all over the place for overflow.

    private long handle;
    private boolean closed;
    private ErrorCondition error;

    //----- Query the state of the Header object -----------------------------//

    public boolean isEmpty() {
        return modified == 0;
    }

    public int getElementCount() {
        return 32 - Integer.numberOfLeadingZeros(modified);
    }

    public boolean hasHandle() {
        return (modified & HANDLE) == HANDLE;
    }

    public boolean hasClosed() {
        return (modified & CLOSED) == CLOSED;
    }

    public boolean hasError() {
        return (modified & ERROR) == ERROR;
    }

    //----- Access to the member data with state checks

    public long getHandle() {
        return handle;
    }

    public Detach setHandle(long handle) {
        if (handle < 0 || handle > UINT_MAX) {
            throw new IllegalArgumentException("The Handle value given is out of range: " + handle);
        } else {
            modified |= HANDLE;
        }

        this.handle = handle;
        return this;
    }

    public boolean getClosed() {
        return closed;
    }

    public Detach setClosed(boolean closed) {
        this.modified |= CLOSED;
        this.closed = closed;
        return this;
    }

    public ErrorCondition getError() {
        return error;
    }

    public Detach setError(ErrorCondition error) {
        if (error != null) {
            modified |= ERROR;
        } else {
            modified &= ~ERROR;
        }
        this.error = error;
        return this;
    }

    @Override
    public Detach copy() {
        Detach copy = new Detach();

        copy.handle = handle;
        copy.closed = closed;
        copy.error = error == null ? null : error.copy();
        copy.modified = modified;

        return copy;
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.DETACH;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleDetach(this, payload, channel, context);
    }

    @Override
    public String toString() {
        return "Detach{" +
               "handle=" + (hasHandle() ? handle : "null") +
               ", closed=" + (hasClosed() ? closed : "null") +
               ", error=" + error +
               '}';
    }
}

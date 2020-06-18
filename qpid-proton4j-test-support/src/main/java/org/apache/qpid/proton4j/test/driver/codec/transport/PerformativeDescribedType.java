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
package org.apache.qpid.proton4j.test.driver.codec.transport;

import java.util.List;

import org.apache.qpid.proton4j.buffer.ProtonBuffer;
import org.apache.qpid.proton4j.test.driver.codec.ListDescribedType;

/**
 * AMQP Performative marker class for DescribedType elements in this codec.
 */
public abstract class PerformativeDescribedType extends ListDescribedType {

    public enum PerformativeType {
        ATTACH,
        BEGIN,
        CLOSE,
        DETACH,
        DISPOSITION,
        END,
        FLOW,
        OPEN,
        TRANSFER,
        HEARTBEAT
    }

    public PerformativeDescribedType(int numberOfFields) {
        super(numberOfFields);
    }

    public PerformativeDescribedType(int numberOfFields, List<Object> described) {
        super(numberOfFields, described);
    }

    public abstract PerformativeType getPerformativeType();

    public interface PerformativeHandler<E> {

        default void handleOpen(Open open, ProtonBuffer payload, int channel, E context) {
            throw new AssertionError("AMQP Open was not handled");
        }
        default void handleBegin(Begin begin, ProtonBuffer payload, int channel, E context) {
            throw new AssertionError("AMQP Begin was not handled");
        }
        default void handleAttach(Attach attach, ProtonBuffer payload, int channel, E context) {
            throw new AssertionError("AMQP Attach was not handled");
        }
        default void handleFlow(Flow flow, ProtonBuffer payload, int channel, E context) {
            throw new AssertionError("AMQP Flow was not handled");
        }
        default void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, E context) {
            throw new AssertionError("AMQP Transfer was not handled");
        }
        default void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, E context) {
            throw new AssertionError("AMQP Disposition was not handled");
        }
        default void handleDetach(Detach detach, ProtonBuffer payload, int channel, E context) {
            throw new AssertionError("AMQP Detach was not handled");
        }
        default void handleEnd(End end, ProtonBuffer payload, int channel, E context) {
            throw new AssertionError("AMQP End was not handled");
        }
        default void handleClose(Close close, ProtonBuffer payload, int channel, E context) {
            throw new AssertionError("AMQP Close was not handled");
        }
        default void handleHeartBeat(HeartBeat thump, ProtonBuffer payload, int channel, E context) {
            throw new AssertionError("AMQP Heart Beat frame was not handled");
        }
    }

    public Object getFieldValueOrSpecDefault(int index) {
        return getFieldValue(index);
    }

    public abstract <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context);

}

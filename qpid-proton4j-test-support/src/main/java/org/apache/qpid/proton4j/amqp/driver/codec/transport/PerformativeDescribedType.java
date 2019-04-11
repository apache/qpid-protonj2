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
package org.apache.qpid.proton4j.amqp.driver.codec.transport;

import java.util.List;

import org.apache.qpid.proton4j.amqp.driver.codec.ListDescribedType;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * AMQP Performative marker class for DescribedType elements in this codec.
 */
public abstract class PerformativeDescribedType extends ListDescribedType {

    enum PerformativeType {
        ATTACH,
        BEGIN,
        CLOSE,
        DETACH,
        DISPOSITION,
        END,
        FLOW,
        OPEN,
        TRANSFER
    }

    public PerformativeDescribedType(int numberOfFields) {
        super(numberOfFields);
    }

    public PerformativeDescribedType(int numberOfFields, List<Object> described) {
        super(numberOfFields, described);
    }

    public abstract PerformativeType getPerformativeType();

    interface PerformativeHandler<E> {

        default void handleOpen(Open open, ProtonBuffer payload, int channel, E context) {}
        default void handleBegin(Begin begin, ProtonBuffer payload, int channel, E context) {}
        default void handleAttach(Attach attach, ProtonBuffer payload, int channel, E context) {}
        default void handleFlow(Flow flow, ProtonBuffer payload, int channel, E context) {}
        default void handleTransfer(Transfer transfer, ProtonBuffer payload, int channel, E context) {}
        default void handleDisposition(Disposition disposition, ProtonBuffer payload, int channel, E context) {}
        default void handleDetach(Detach detach, ProtonBuffer payload, int channel, E context) {}
        default void handleEnd(End end, ProtonBuffer payload, int channel, E context) {}
        default void handleClose(Close close, ProtonBuffer payload, int channel, E context) {}

    }

    public abstract <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context);

}

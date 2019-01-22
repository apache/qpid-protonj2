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

import org.apache.qpid.proton4j.buffer.ProtonBuffer;

/**
 * Marker interface for AMQP Performatives
 */
public interface Performative {

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

    PerformativeType getPerformativeType();

    Performative copy();

    interface PerformativeHandler<E> {

        default void handleOpen(Open open, ProtonBuffer payload, E context) {}
        default void handleBegin(Begin begin, ProtonBuffer payload, E context) {}
        default void handleAttach(Attach attach, ProtonBuffer payload, E context) {}
        default void handleFlow(Flow flow, ProtonBuffer payload, E context) {}
        default void handleTransfer(Transfer transfer, ProtonBuffer payload, E context) {}
        default void handleDisposition(Disposition disposition, ProtonBuffer payload, E context) {}
        default void handleDetach(Detach detach, ProtonBuffer payload, E context) {}
        default void handleEnd(End end, ProtonBuffer payload, E context) {}
        default void handleClose(Close close, ProtonBuffer payload, E context) {}

    }

    <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, E context);

}

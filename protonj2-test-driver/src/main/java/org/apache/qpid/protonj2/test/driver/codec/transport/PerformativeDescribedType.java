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
package org.apache.qpid.protonj2.test.driver.codec.transport;

import java.util.List;

import org.apache.qpid.protonj2.test.driver.codec.ListDescribedType;

import io.netty.buffer.ByteBuf;

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

        default void handleOpen(int frameSize, Open open, ByteBuf payload, int channel, E context) {
            throw new AssertionError("AMQP Open was not handled");
        }
        default void handleBegin(int frameSize, Begin begin, ByteBuf payload, int channel, E context) {
            throw new AssertionError("AMQP Begin was not handled");
        }
        default void handleAttach(int frameSize, Attach attach, ByteBuf payload, int channel, E context) {
            throw new AssertionError("AMQP Attach was not handled");
        }
        default void handleFlow(int frameSize, Flow flow, ByteBuf payload, int channel, E context) {
            throw new AssertionError("AMQP Flow was not handled");
        }
        default void handleTransfer(int frameSize, Transfer transfer, ByteBuf payload, int channel, E context) {
            throw new AssertionError("AMQP Transfer was not handled");
        }
        default void handleDisposition(int frameSize, Disposition disposition, ByteBuf payload, int channel, E context) {
            throw new AssertionError("AMQP Disposition was not handled");
        }
        default void handleDetach(int frameSize, Detach detach, ByteBuf payload, int channel, E context) {
            throw new AssertionError("AMQP Detach was not handled");
        }
        default void handleEnd(int frameSize, End end, ByteBuf payload, int channel, E context) {
            throw new AssertionError("AMQP End was not handled");
        }
        default void handleClose(int frameSize, Close close, ByteBuf payload, int channel, E context) {
            throw new AssertionError("AMQP Close was not handled");
        }
        default void handleHeartBeat(int frameSize, HeartBeat thump, ByteBuf payload, int channel, E context) {
            throw new AssertionError("AMQP Heart Beat frame was not handled");
        }
    }

    public Object getFieldValueOrSpecDefault(int index) {
        return getFieldValue(index);
    }

    public abstract <E> void invoke(PerformativeHandler<E> handler, int frameSize, ByteBuf payload, int channel, E context);

    @Override
    public String toString() {
        return getPerformativeType() + " " + getList();
    }
}

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

import org.apache.qpid.proton4j.amqp.Binary;
import org.apache.qpid.proton4j.amqp.DescribedType;
import org.apache.qpid.proton4j.amqp.Symbol;
import org.apache.qpid.proton4j.amqp.UnsignedByte;
import org.apache.qpid.proton4j.amqp.UnsignedInteger;
import org.apache.qpid.proton4j.amqp.UnsignedLong;
import org.apache.qpid.proton4j.buffer.ProtonBuffer;

public class Transfer extends PerformativeDescribedType {

    public static final Symbol DESCRIPTOR_SYMBOL = Symbol.valueOf("amqp:transfer:list");
    public static final UnsignedLong DESCRIPTOR_CODE = UnsignedLong.valueOf(0x0000000000000014L);

    /**
     * Enumeration which maps to fields in the Transfer Performative
     */
    public enum Field {
        HANDLE,
        DELIVERY_ID,
        DELIVERY_TAG,
        MESSAGE_FORMAT,
        SETTLED,
        MORE,
        RCV_SETTLE_MODE,
        STATE,
        RESUME,
        ABORTED,
        BATCHABLE
    }

    public Transfer() {
        super(Field.values().length);
    }

    @SuppressWarnings("unchecked")
    public Transfer(Object described) {
        super(Field.values().length, (List<Object>) described);
    }

    public Transfer(List<Object> described) {
        super(Field.values().length, described);
    }

    @Override
    public Symbol getDescriptor() {
        return DESCRIPTOR_SYMBOL;
    }

    public Transfer setHandle(UnsignedInteger o) {
        getList().set(Field.HANDLE.ordinal(), o);
        return this;
    }

    public UnsignedInteger getHandle() {
        return (UnsignedInteger) getList().get(Field.HANDLE.ordinal());
    }

    public Transfer setDeliveryId(UnsignedInteger o) {
        getList().set(Field.DELIVERY_ID.ordinal(), o);
        return this;
    }

    public UnsignedInteger getDeliveryId() {
        return (UnsignedInteger) getList().get(Field.DELIVERY_ID.ordinal());
    }

    public Transfer setDeliveryTag(Binary o) {
        getList().set(Field.DELIVERY_TAG.ordinal(), o);
        return this;
    }

    public Binary getDeliveryTag() {
        return (Binary) getList().get(Field.DELIVERY_TAG.ordinal());
    }

    public Transfer setMessageFormat(UnsignedInteger o) {
        getList().set(Field.MESSAGE_FORMAT.ordinal(), o);
        return this;
    }

    public UnsignedInteger getMessageFormat() {
        return (UnsignedInteger) getList().get(Field.MESSAGE_FORMAT.ordinal());
    }

    public Transfer setSettled(Boolean o) {
        getList().set(Field.SETTLED.ordinal(), o);
        return this;
    }

    public Boolean getSettled() {
        return (Boolean) getList().get(Field.SETTLED.ordinal());
    }

    public Transfer setMore(Boolean o) {
        getList().set(Field.MORE.ordinal(), o);
        return this;
    }

    public Boolean getMore() {
        return (Boolean) getList().get(Field.MORE.ordinal());
    }

    public Transfer setRcvSettleMode(UnsignedByte o) {
        getList().set(Field.RCV_SETTLE_MODE.ordinal(), o);
        return this;
    }

    public UnsignedByte getRcvSettleMode() {
        return (UnsignedByte) getList().get(Field.RCV_SETTLE_MODE.ordinal());
    }

    public Transfer setState(DescribedType o) {
        getList().set(Field.STATE.ordinal(), o);
        return this;
    }

    public DescribedType getState() {
        return (DescribedType) getList().get(Field.STATE.ordinal());
    }

    public Transfer setResume(Boolean o) {
        getList().set(Field.RESUME.ordinal(), o);
        return this;
    }

    public Boolean getResume() {
        return (Boolean) getList().get(Field.RESUME.ordinal());
    }

    public Transfer setAborted(Boolean o) {
        getList().set(Field.ABORTED.ordinal(), o);
        return this;
    }

    public Boolean getAborted() {
        return (Boolean) getList().get(Field.ABORTED.ordinal());
    }

    public Transfer setBatchable(Boolean o) {
        getList().set(Field.BATCHABLE.ordinal(), o);
        return this;
    }

    public Boolean getBatchable() {
        return (Boolean) getList().get(Field.BATCHABLE.ordinal());
    }

    @Override
    public PerformativeType getPerformativeType() {
        return PerformativeType.TRANSFER;
    }

    @Override
    public <E> void invoke(PerformativeHandler<E> handler, ProtonBuffer payload, int channel, E context) {
        handler.handleTransfer(this, payload, channel, context);
    }
}

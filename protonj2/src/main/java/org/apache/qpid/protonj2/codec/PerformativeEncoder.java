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

package org.apache.qpid.protonj2.codec;

import org.apache.qpid.protonj2.buffer.ProtonBuffer;
import org.apache.qpid.protonj2.types.transport.Attach;
import org.apache.qpid.protonj2.types.transport.Begin;
import org.apache.qpid.protonj2.types.transport.Close;
import org.apache.qpid.protonj2.types.transport.Detach;
import org.apache.qpid.protonj2.types.transport.Disposition;
import org.apache.qpid.protonj2.types.transport.End;
import org.apache.qpid.protonj2.types.transport.Flow;
import org.apache.qpid.protonj2.types.transport.Open;
import org.apache.qpid.protonj2.types.transport.Performative.PerformativeHandler;
import org.apache.qpid.protonj2.types.transport.Transfer;

/**
 * AMQP Performative type specific encoder that uses any {@link Encoder} to
 * cache the specific type encoders for various section types and use them
 * directly instead of looking them up via calls to
 * {@link Encoder#writeObject(org.apache.qpid.protonj2.buffer.ProtonBuffer, EncoderState, Object)}
 */
public final class PerformativeEncoder implements PerformativeHandler<Encoder> {

    private final TypeEncoder<Attach> attachEncoder;
    private final TypeEncoder<Begin> beginEncoder;
    private final TypeEncoder<Close> closeEncoder;
    private final TypeEncoder<Detach> detachEncoder;
    private final TypeEncoder<Disposition> dispositionEncoder;
    private final TypeEncoder<End> endEncoder;
    private final TypeEncoder<Flow> flowEncoder;
    private final TypeEncoder<Open> openEncoder;
    private final TypeEncoder<Transfer> transferEncoder;

    private final Encoder encoder;
    private final EncoderState encoderState;

    @SuppressWarnings("unchecked")
    public PerformativeEncoder(Encoder encoder) {
        this.encoder = encoder;
        this.encoderState = encoder.newEncoderState();

        attachEncoder = (TypeEncoder<Attach>) encoder.getTypeEncoder(Attach.class);
        beginEncoder = (TypeEncoder<Begin>) encoder.getTypeEncoder(Begin.class);
        closeEncoder = (TypeEncoder<Close>) encoder.getTypeEncoder(Close.class);
        detachEncoder = (TypeEncoder<Detach>) encoder.getTypeEncoder(Detach.class);
        dispositionEncoder = (TypeEncoder<Disposition>) encoder.getTypeEncoder(Disposition.class);
        endEncoder = (TypeEncoder<End>) encoder.getTypeEncoder(End.class);
        flowEncoder = (TypeEncoder<Flow>) encoder.getTypeEncoder(Flow.class);
        openEncoder = (TypeEncoder<Open>) encoder.getTypeEncoder(Open.class);
        transferEncoder = (TypeEncoder<Transfer>) encoder.getTypeEncoder(Transfer.class);
    }

    public Encoder getEncoder() {
        return encoder;
    }

    public EncoderState getEncoderState() {
        return encoderState;
    }

    @Override
    public void handleOpen(Open open, ProtonBuffer target, int channel, Encoder encoder) {
        try {
            openEncoder.writeType(target, encoderState, open);
        } finally {
            encoderState.reset();
        }
    }

    @Override
    public void handleBegin(Begin begin, ProtonBuffer target, int channel, Encoder encoder) {
        try {
            beginEncoder.writeType(target, encoderState, begin);
        } finally {
            encoderState.reset();
        }
    }

    @Override
    public void handleAttach(Attach attach, ProtonBuffer target, int channel, Encoder encoder) {
        try {
            attachEncoder.writeType(target, encoderState, attach);
        } finally {
            encoderState.reset();
        }
    }

    @Override
    public void handleFlow(Flow flow, ProtonBuffer target, int channel, Encoder encoder) {
        try {
            flowEncoder.writeType(target, encoderState, flow);
        } finally {
            encoderState.reset();
        }
    }

    @Override
    public void handleTransfer(Transfer transfer, ProtonBuffer target, int channel, Encoder encoder) {
        try {
            transferEncoder.writeType(target, encoderState, transfer);
        } finally {
            encoderState.reset();
        }
    }

    @Override
    public void handleDisposition(Disposition disposition, ProtonBuffer target, int channel, Encoder encoder) {
        try {
            dispositionEncoder.writeType(target, encoderState, disposition);
        } finally {
            encoderState.reset();
        }
    }

    @Override
    public void handleDetach(Detach detach, ProtonBuffer target, int channel, Encoder encoder) {
        try {
            detachEncoder.writeType(target, encoderState, detach);
        } finally {
            encoderState.reset();
        }
    }

    @Override
    public void handleEnd(End end, ProtonBuffer target, int channel, Encoder encoder) {
        try {
            endEncoder.writeType(target, encoderState, end);
        } finally {
            encoderState.reset();
        }
    }

    @Override
    public void handleClose(Close close, ProtonBuffer target, int channel, Encoder encoder) {
        try {
            closeEncoder.writeType(target, encoderState, close);
        } finally {
            encoderState.reset();
        }
    }
}

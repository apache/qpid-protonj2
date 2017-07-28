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
package org.apache.qpid.proton4j.codec.encoders;

import org.apache.qpid.proton4j.codec.encoders.messaging.AcceptedTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.AmqpSequenceTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.AmqpValueTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ApplicationPropertiesTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DataTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeleteOnCloseTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeleteOnNoLinksOrMessagesTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeleteOnNoLinksTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeleteOnNoMessagesTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.DeliveryAnnotationsTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.FooterTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.HeaderTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.MessageAnnotationsTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ModifiedTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.PropertiesTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ReceivedTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.RejectedTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.ReleasedTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.SourceTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.messaging.TargetTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ArrayTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.BinaryTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.BooleanTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ByteTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.CharacterTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.Decimal128TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.Decimal32TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.Decimal64TypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.DoubleTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.FloatTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.IntegerTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ListTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.LongTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.MapTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.NullTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.ShortTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.StringTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.SymbolTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.TimestampTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UUIDTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedByteTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedIntegerTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedLongTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.primitives.UnsignedShortTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.CoordinatorTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.DeclareTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.DeclaredTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.DischargeTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transactions.TransactionStateTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.AttachTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.BeginTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.CloseTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.DetachTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.DispositionTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.EndTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.ErrorConditionTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.FlowTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.OpenTypeEncoder;
import org.apache.qpid.proton4j.codec.encoders.transport.TransferTypeEncoder;

/**
 * Factory that create and initializes new BuiltinEncoder instances
 */
public class ProtonEncoderFactory {

    private ProtonEncoderFactory() {
    }

    public static ProtonEncoder create() {
        ProtonEncoder encoder = new ProtonEncoder();

        addPrimitiveTypeEncoders(encoder);
        addMessagingTypeEncoders(encoder);
        addTransactionTypeEncoders(encoder);
        addTransportTypeEncoders(encoder);

        return encoder;
    }

    private static void addMessagingTypeEncoders(ProtonEncoder encoder) {
        encoder.registerTypeEncoder(new AcceptedTypeEncoder());
        encoder.registerTypeEncoder(new AmqpSequenceTypeEncoder());
        encoder.registerTypeEncoder(new AmqpValueTypeEncoder());
        encoder.registerTypeEncoder(new ApplicationPropertiesTypeEncoder());
        encoder.registerTypeEncoder(new DataTypeEncoder());
        encoder.registerTypeEncoder(new DeleteOnCloseTypeEncoder());
        encoder.registerTypeEncoder(new DeleteOnNoLinksOrMessagesTypeEncoder());
        encoder.registerTypeEncoder(new DeleteOnNoLinksTypeEncoder());
        encoder.registerTypeEncoder(new DeleteOnNoMessagesTypeEncoder());
        encoder.registerTypeEncoder(new DeliveryAnnotationsTypeEncoder());
        encoder.registerTypeEncoder(new FooterTypeEncoder());
        encoder.registerTypeEncoder(new HeaderTypeEncoder());
        encoder.registerTypeEncoder(new MessageAnnotationsTypeEncoder());
        encoder.registerTypeEncoder(new ModifiedTypeEncoder());
        encoder.registerTypeEncoder(new PropertiesTypeEncoder());
        encoder.registerTypeEncoder(new ReceivedTypeEncoder());
        encoder.registerTypeEncoder(new RejectedTypeEncoder());
        encoder.registerTypeEncoder(new ReleasedTypeEncoder());
        encoder.registerTypeEncoder(new SourceTypeEncoder());
        encoder.registerTypeEncoder(new TargetTypeEncoder());
    }

    private static void addTransactionTypeEncoders(ProtonEncoder encoder) {
        encoder.registerTypeEncoder(new CoordinatorTypeEncoder());
        encoder.registerTypeEncoder(new DeclaredTypeEncoder());
        encoder.registerTypeEncoder(new DeclareTypeEncoder());
        encoder.registerTypeEncoder(new DischargeTypeEncoder());
        encoder.registerTypeEncoder(new TransactionStateTypeEncoder());
    }

    private static void addTransportTypeEncoders(ProtonEncoder encoder) {
        encoder.registerTypeEncoder(new AttachTypeEncoder());
        encoder.registerTypeEncoder(new BeginTypeEncoder());
        encoder.registerTypeEncoder(new CloseTypeEncoder());
        encoder.registerTypeEncoder(new DetachTypeEncoder());
        encoder.registerTypeEncoder(new DispositionTypeEncoder());
        encoder.registerTypeEncoder(new EndTypeEncoder());
        encoder.registerTypeEncoder(new ErrorConditionTypeEncoder());
        encoder.registerTypeEncoder(new FlowTypeEncoder());
        encoder.registerTypeEncoder(new OpenTypeEncoder());
        encoder.registerTypeEncoder(new TransferTypeEncoder());
    }

    private static void addPrimitiveTypeEncoders(ProtonEncoder encoder) {
        encoder.registerTypeEncoder(new ArrayTypeEncoder());
        encoder.registerTypeEncoder(new BinaryTypeEncoder());
        encoder.registerTypeEncoder(new BooleanTypeEncoder());
        encoder.registerTypeEncoder(new ByteTypeEncoder());
        encoder.registerTypeEncoder(new CharacterTypeEncoder());
        encoder.registerTypeEncoder(new Decimal32TypeEncoder());
        encoder.registerTypeEncoder(new Decimal64TypeEncoder());
        encoder.registerTypeEncoder(new Decimal128TypeEncoder());
        encoder.registerTypeEncoder(new DoubleTypeEncoder());
        encoder.registerTypeEncoder(new FloatTypeEncoder());
        encoder.registerTypeEncoder(new IntegerTypeEncoder());
        encoder.registerTypeEncoder(new ListTypeEncoder());
        encoder.registerTypeEncoder(new LongTypeEncoder());
        encoder.registerTypeEncoder(new MapTypeEncoder());
        encoder.registerTypeEncoder(new NullTypeEncoder());
        encoder.registerTypeEncoder(new ShortTypeEncoder());
        encoder.registerTypeEncoder(new StringTypeEncoder());
        encoder.registerTypeEncoder(new SymbolTypeEncoder());
        encoder.registerTypeEncoder(new TimestampTypeEncoder());
        encoder.registerTypeEncoder(new UnsignedByteTypeEncoder());
        encoder.registerTypeEncoder(new UnsignedShortTypeEncoder());
        encoder.registerTypeEncoder(new UnsignedIntegerTypeEncoder());
        encoder.registerTypeEncoder(new UnsignedLongTypeEncoder());
        encoder.registerTypeEncoder(new UUIDTypeEncoder());
    }
}

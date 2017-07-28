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
package org.apache.qpid.proton4j.codec.decoders;

import org.apache.qpid.proton4j.codec.decoders.messaging.AcceptedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.AmqpSequenceTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.AmqpValueTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.ApplicationPropertiesTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DataTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeleteOnCloseTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeleteOnNoLinksOrMessagesTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeleteOnNoLinksTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeleteOnNoMessagesTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.DeliveryAnnotationsTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.FooterTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.HeaderTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.MessageAnnotationsTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.ModifiedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.PropertiesTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.ReceivedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.RejectedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.ReleasedTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.SourceTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.messaging.TargetTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Array32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Array8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Binary32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Binary8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.BooleanFalseTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.BooleanTrueTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.BooleanTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ByteTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.CharacterTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Decimal128TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Decimal32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Decimal64TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.DoubleTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.FloatTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Integer32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Integer8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.List0TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.List32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.List8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Long8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.LongTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Map32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Map8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.NullTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.ShortTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.String32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.String8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Symbol32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.Symbol8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.TimestampTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UUIDTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedByteTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedInteger0TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedInteger32TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedInteger8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedLong0TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedLong64TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedLong8TypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.primitives.UnsignedShortTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.CoordinatorTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.DeclareTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.DeclaredTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.DischargeTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transactions.TransactionStateTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.AttachTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.BeginTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.CloseTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.DetachTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.DispositionTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.EndTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.ErrorConditionTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.FlowTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.OpenTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.transport.TransferTypeDecoder;

/**
 * Factory that create and initializes new BuiltinDecoder instances
 */
public class ProtonDecoderFactory {

    private ProtonDecoderFactory() {
    }

    public static ProtonDecoder create() {
        ProtonDecoder decoder = new ProtonDecoder();

        addPrimitiveDecoders(decoder);
        addMessagingTypeDecoders(decoder);
        addTransactionTypeDecoders(decoder);
        addTransportTypeDecoders(decoder);

        return decoder;
    }

    private static void addMessagingTypeDecoders(ProtonDecoder Decoder) {
        Decoder.registerDescribedTypeDecoder(new AcceptedTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new AmqpSequenceTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new AmqpValueTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new ApplicationPropertiesTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DataTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeleteOnCloseTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeleteOnNoLinksOrMessagesTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeleteOnNoLinksTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeleteOnNoMessagesTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeliveryAnnotationsTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new FooterTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new HeaderTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new MessageAnnotationsTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new ModifiedTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new PropertiesTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new ReceivedTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new RejectedTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new ReleasedTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new SourceTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new TargetTypeDecoder());
    }

    private static void addTransactionTypeDecoders(ProtonDecoder Decoder) {
        Decoder.registerDescribedTypeDecoder(new CoordinatorTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeclaredTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DeclareTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DischargeTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new TransactionStateTypeDecoder());
    }

    private static void addTransportTypeDecoders(ProtonDecoder Decoder) {
        Decoder.registerDescribedTypeDecoder(new AttachTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new BeginTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new CloseTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DetachTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new DispositionTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new EndTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new ErrorConditionTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new FlowTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new OpenTypeDecoder());
        Decoder.registerDescribedTypeDecoder(new TransferTypeDecoder());
    }

    private static void addPrimitiveDecoders(ProtonDecoder decoder) {
        decoder.registerPrimitiveTypeDecoder(new BooleanTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new BooleanFalseTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new BooleanTrueTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Binary32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Binary8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new ByteTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new CharacterTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Decimal32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Decimal64TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Decimal128TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new DoubleTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new FloatTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new NullTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedByteTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new ShortTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedShortTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Integer8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Integer32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedInteger32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedInteger0TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedInteger8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new LongTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Long8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedLong64TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedLong0TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UnsignedLong8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new String32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new String8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Symbol8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Symbol32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new UUIDTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new TimestampTypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new List0TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new List8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new List32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Map8TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Map32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Array32TypeDecoder());
        decoder.registerPrimitiveTypeDecoder(new Array8TypeDecoder());
    }
}

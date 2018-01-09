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
import org.apache.qpid.proton4j.codec.decoders.security.SaslChallengeTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.security.SaslInitTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.security.SaslMechanismsTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.security.SaslOutcomeTypeDecoder;
import org.apache.qpid.proton4j.codec.decoders.security.SaslResponseTypeDecoder;
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

    public static ProtonDecoder createSasl() {
        ProtonDecoder decoder = new ProtonDecoder();

        addSaslTypeDecoders(decoder);
        addPrimitiveDecoders(decoder);

        return decoder;
    }

    private static void addMessagingTypeDecoders(ProtonDecoder Decoder) {
        Decoder.registerTypeDecoder(new AcceptedTypeDecoder());
        Decoder.registerTypeDecoder(new AmqpSequenceTypeDecoder());
        Decoder.registerTypeDecoder(new AmqpValueTypeDecoder());
        Decoder.registerTypeDecoder(new ApplicationPropertiesTypeDecoder());
        Decoder.registerTypeDecoder(new DataTypeDecoder());
        Decoder.registerTypeDecoder(new DeleteOnCloseTypeDecoder());
        Decoder.registerTypeDecoder(new DeleteOnNoLinksOrMessagesTypeDecoder());
        Decoder.registerTypeDecoder(new DeleteOnNoLinksTypeDecoder());
        Decoder.registerTypeDecoder(new DeleteOnNoMessagesTypeDecoder());
        Decoder.registerTypeDecoder(new DeliveryAnnotationsTypeDecoder());
        Decoder.registerTypeDecoder(new FooterTypeDecoder());
        Decoder.registerTypeDecoder(new HeaderTypeDecoder());
        Decoder.registerTypeDecoder(new MessageAnnotationsTypeDecoder());
        Decoder.registerTypeDecoder(new ModifiedTypeDecoder());
        Decoder.registerTypeDecoder(new PropertiesTypeDecoder());
        Decoder.registerTypeDecoder(new ReceivedTypeDecoder());
        Decoder.registerTypeDecoder(new RejectedTypeDecoder());
        Decoder.registerTypeDecoder(new ReleasedTypeDecoder());
        Decoder.registerTypeDecoder(new SourceTypeDecoder());
        Decoder.registerTypeDecoder(new TargetTypeDecoder());
    }

    private static void addTransactionTypeDecoders(ProtonDecoder Decoder) {
        Decoder.registerTypeDecoder(new CoordinatorTypeDecoder());
        Decoder.registerTypeDecoder(new DeclaredTypeDecoder());
        Decoder.registerTypeDecoder(new DeclareTypeDecoder());
        Decoder.registerTypeDecoder(new DischargeTypeDecoder());
        Decoder.registerTypeDecoder(new TransactionStateTypeDecoder());
    }

    private static void addTransportTypeDecoders(ProtonDecoder Decoder) {
        Decoder.registerTypeDecoder(new AttachTypeDecoder());
        Decoder.registerTypeDecoder(new BeginTypeDecoder());
        Decoder.registerTypeDecoder(new CloseTypeDecoder());
        Decoder.registerTypeDecoder(new DetachTypeDecoder());
        Decoder.registerTypeDecoder(new DispositionTypeDecoder());
        Decoder.registerTypeDecoder(new EndTypeDecoder());
        Decoder.registerTypeDecoder(new ErrorConditionTypeDecoder());
        Decoder.registerTypeDecoder(new FlowTypeDecoder());
        Decoder.registerTypeDecoder(new OpenTypeDecoder());
        Decoder.registerTypeDecoder(new TransferTypeDecoder());
    }

    private static void addPrimitiveDecoders(ProtonDecoder decoder) {
        decoder.registerTypeDecoder(new BooleanTypeDecoder());
        decoder.registerTypeDecoder(new BooleanFalseTypeDecoder());
        decoder.registerTypeDecoder(new BooleanTrueTypeDecoder());
        decoder.registerTypeDecoder(new Binary32TypeDecoder());
        decoder.registerTypeDecoder(new Binary8TypeDecoder());
        decoder.registerTypeDecoder(new ByteTypeDecoder());
        decoder.registerTypeDecoder(new CharacterTypeDecoder());
        decoder.registerTypeDecoder(new Decimal32TypeDecoder());
        decoder.registerTypeDecoder(new Decimal64TypeDecoder());
        decoder.registerTypeDecoder(new Decimal128TypeDecoder());
        decoder.registerTypeDecoder(new DoubleTypeDecoder());
        decoder.registerTypeDecoder(new FloatTypeDecoder());
        decoder.registerTypeDecoder(new NullTypeDecoder());
        decoder.registerTypeDecoder(new UnsignedByteTypeDecoder());
        decoder.registerTypeDecoder(new ShortTypeDecoder());
        decoder.registerTypeDecoder(new UnsignedShortTypeDecoder());
        decoder.registerTypeDecoder(new Integer8TypeDecoder());
        decoder.registerTypeDecoder(new Integer32TypeDecoder());
        decoder.registerTypeDecoder(new UnsignedInteger32TypeDecoder());
        decoder.registerTypeDecoder(new UnsignedInteger0TypeDecoder());
        decoder.registerTypeDecoder(new UnsignedInteger8TypeDecoder());
        decoder.registerTypeDecoder(new LongTypeDecoder());
        decoder.registerTypeDecoder(new Long8TypeDecoder());
        decoder.registerTypeDecoder(new UnsignedLong64TypeDecoder());
        decoder.registerTypeDecoder(new UnsignedLong0TypeDecoder());
        decoder.registerTypeDecoder(new UnsignedLong8TypeDecoder());
        decoder.registerTypeDecoder(new String32TypeDecoder());
        decoder.registerTypeDecoder(new String8TypeDecoder());
        decoder.registerTypeDecoder(new Symbol8TypeDecoder());
        decoder.registerTypeDecoder(new Symbol32TypeDecoder());
        decoder.registerTypeDecoder(new UUIDTypeDecoder());
        decoder.registerTypeDecoder(new TimestampTypeDecoder());
        decoder.registerTypeDecoder(new List0TypeDecoder());
        decoder.registerTypeDecoder(new List8TypeDecoder());
        decoder.registerTypeDecoder(new List32TypeDecoder());
        decoder.registerTypeDecoder(new Map8TypeDecoder());
        decoder.registerTypeDecoder(new Map32TypeDecoder());
        decoder.registerTypeDecoder(new Array32TypeDecoder());
        decoder.registerTypeDecoder(new Array8TypeDecoder());
    }

    private static void addSaslTypeDecoders(ProtonDecoder decoder) {
        decoder.registerTypeDecoder(new SaslChallengeTypeDecoder());
        decoder.registerTypeDecoder(new SaslInitTypeDecoder());
        decoder.registerTypeDecoder(new SaslMechanismsTypeDecoder());
        decoder.registerTypeDecoder(new SaslOutcomeTypeDecoder());
        decoder.registerTypeDecoder(new SaslResponseTypeDecoder());
    }
}

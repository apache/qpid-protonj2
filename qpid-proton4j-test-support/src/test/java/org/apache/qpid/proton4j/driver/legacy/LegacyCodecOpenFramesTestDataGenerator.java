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
package org.apache.qpid.proton4j.driver.legacy;

import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.transport.Open;

/**
 * Generates the test data used to create a tests for codec that read
 * Frames encoded using the proton-j framework.
 */
public class LegacyCodecOpenFramesTestDataGenerator {

    public static void main(String[] args) {
        // 1: Empty Open - No fields set
        Open emptyOpen = new Open();
        String emptyOpenFrameString = LegacyFrmaeDataGenerator.generateUnitTestVariable("emptyOpen", emptyOpen);
        System.out.println(emptyOpenFrameString);

        // 2: Basic Open - No capabilities or locals set
        Open basicOpen = new Open();
        basicOpen.setContainerId("container");
        basicOpen.setHostname("localhost");
        basicOpen.setMaxFrameSize(UnsignedInteger.valueOf(16384));
        basicOpen.setIdleTimeOut(UnsignedInteger.valueOf(30000));
        String basicOpenString = LegacyFrmaeDataGenerator.generateUnitTestVariable("basicOpen", basicOpen);
        System.out.println(basicOpenString);
    }
}

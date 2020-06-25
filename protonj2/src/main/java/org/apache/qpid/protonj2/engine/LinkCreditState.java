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
package org.apache.qpid.protonj2.engine;

/**
 * State object holding information about the current link credit
 */
public interface LinkCreditState {

    /**
     * The currently available credit for this link
     *
     * @return the current amount of link credit
     */
    int getCredit();

    /**
     * The current delivery count value for this link
     *
     * @return the current delivery count value for the link.
     */
    int getDeliveryCount();

    /**
     * @return true if the link drain is active.
     */
    boolean isDrain();

    /**
     * @return true if the link has been requested to echo its state.
     */
    boolean isEcho();

}

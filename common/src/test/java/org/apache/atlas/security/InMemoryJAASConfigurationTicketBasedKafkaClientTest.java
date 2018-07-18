/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.security;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.fail;

@Test
public class InMemoryJAASConfigurationTicketBasedKafkaClientTest {

    private static final String ATLAS_JAAS_PROP_FILE = "atlas-jaas.properties";

    @BeforeMethod
    protected void setUp() throws Exception {
        try {
            InMemoryJAASConfiguration.init(ATLAS_JAAS_PROP_FILE);
            InMemoryJAASConfiguration.setConfigSectionRedirect("KafkaClient", "ticketBased-KafkaClient");
        } catch (Throwable t) {
            fail("InMemoryJAASConfiguration.init() is not expected to throw Exception:" + t);
        }
    }


    @Test
    public void testGetAppConfigurationEntryStringForticketBasedKafkaClient() {

        AppConfigurationEntry[] entries =
                Configuration.getConfiguration().getAppConfigurationEntry("KafkaClient");
        assertNotNull(entries);
        assertEquals((String) entries[0].getOptions().get("useTicketCache"), "true");
    }


}


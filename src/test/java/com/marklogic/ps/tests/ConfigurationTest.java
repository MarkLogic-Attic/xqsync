/**
 * Copyright (c) 2007-2017 MarkLogic Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.ps.tests;

import java.util.Properties;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.xqsync.Configuration;
import com.marklogic.ps.xqsync.XQSync;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class ConfigurationTest {
    @Test
    public void testConfiguration() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(Configuration.CONFIGURATION_CLASSNAME_KEY,
                ExampleConfiguration.class.getCanonicalName());
        properties.setProperty(Configuration.INPUT_CONNECTION_STRING_KEY,
                "test");
        Configuration config = XQSync.initConfiguration(SimpleLogger
                .getSimpleLogger(), properties);
        config.close();
        assertEquals(config.getClass().getCanonicalName(),
                ExampleConfiguration.class.getCanonicalName());
    }

}

/**
 * Copyright (c) 2007-2022 MarkLogic Corporation. All rights reserved.
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
package com.marklogic.ps.xqsync;

import com.marklogic.ps.xqsync.Configuration;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 *         This subclass can override Configuration.getWriter(), etc.
 */
public class ExampleConfiguration extends Configuration {

    @Override
    public void close() {
        logger.info("closing");
        super.close();
    }

    @Override
    public void configure() {
        logger.info("overriding superclass configure method");
    }
}

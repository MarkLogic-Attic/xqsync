/**
 * Copyright (c) 2010 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.tests;

import com.marklogic.ps.xqsync.Configuration;

/**
 * @author Michael Blakeley, Mark Logic Corporation
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
    public void configure() throws Exception {
        logger.info("overriding superclass configure method");
    }
}

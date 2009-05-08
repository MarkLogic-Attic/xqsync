/**
 * Copyright (c) 2009 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.tests;

import com.marklogic.ps.Session;
import com.marklogic.ps.xqsync.Configuration;
import com.marklogic.ps.xqsync.DocumentInterface;
import com.marklogic.ps.xqsync.SessionReader;
import com.marklogic.ps.xqsync.SyncException;
import com.marklogic.xcc.ResultSequence;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class SessionReaderTest extends SessionReader {

    /**
     * @param _configuration
     * @throws SyncException
     */
    public SessionReaderTest(Configuration _configuration)
            throws SyncException {
        super(_configuration);
    }

    @Override
    protected void cleanup(Session session, ResultSequence rs) {
        super.cleanup(session, rs);
    }

    @Override
    public void read(String[] _uris, DocumentInterface _document)
            throws SyncException {
        super.read(_uris, _document);
        for (int i = 0; i < _uris.length; i++) {
            if (null != _uris[i]) {
                logger.info("[" + i + "] " + _uris[i] + " as "
                        + _document.getOutputUri(i));
            }
        }
    }

}

/**
 * Copyright (c) 2009-2010 Mark Logic Corporation. All rights reserved.
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

/**
 * Copyright (c) 2008-2012 MarkLogic Corporation. All rights reserved.
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

import java.io.IOException;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class PackageWriter extends AbstractWriter {

    OutputPackage pkg;

    /**
     * @param _configuration
     * @param _pkg
     * @throws SyncException
     */
    public PackageWriter(Configuration _configuration, OutputPackage _pkg)
            throws SyncException {
        // superclass takes care of some configuration
        super(_configuration);

        pkg = _pkg;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.ps.xqsync.WriterInterface#write(java.lang.String,
     * byte[], com.marklogic.ps.xqsync.XQSyncDocumentMetadata)
     */
    public int write(String _uri, byte[] _bytes,
            XQSyncDocumentMetadata _metadata) throws SyncException {
        if (null == pkg) {
            throw new SyncException("null output package");
        }
        if (null == _bytes) {
            throw new SyncException("null output bytes");
        }
        if (null == _metadata) {
            throw new SyncException("null output metadata");
        }
        try {
            // bytes from one document should not overflow int
            return (int) pkg.write(_uri, _bytes, _metadata);
        } catch (IOException e) {
            throw new SyncException(e);
        }
        // NB - caller must flush() the package
    }

    /**
     * @throws SyncException
     *
     */
    public void close() throws SyncException {
        if (null != pkg) {
            pkg.close();
        }
    }

}

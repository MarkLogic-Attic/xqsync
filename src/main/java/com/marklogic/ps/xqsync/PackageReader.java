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
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class PackageReader extends FilePathReader {

    protected InputPackage pkg;

    /**
     * @param _configuration
     * @param _pkg
     * @throws SyncException
     */
    public PackageReader(Configuration _configuration)
            throws SyncException {
        // superclass takes care of some configuration
        super(_configuration);
    }

    /**
     * @param _pkg
     */
    public void setPackage(InputPackage _pkg) {
        pkg = _pkg;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.ReaderInterface#read(java.lang.String,
     * com.marklogic.ps.xqsync.DocumentInterface)
     */
    @Override
    public void read(String[] _uris, DocumentInterface _document)
            throws SyncException {
        if (null == _uris) {
            throw new SyncException("null paths");
        }
        if (null == _uris[0]) {
            throw new SyncException("empty paths");
        }
        if (null == pkg) {
            throw new SyncException("null input package");
        }

        String uri;

        for (int i = 0; i < _uris.length; i++) {
            uri = _uris[i];

            if (uri == null) {
                continue;
            }

            try {
                // read the content: must work for bin or xml, so use bytes
                _document.setContent(i, pkg.getContent(uri));

                // read the metadata
                MetadataInterface metadata = pkg.getMetadataEntry(uri);
                _document.setMetadata(i, metadata);
            } catch (IOException e) {
                throw new SyncException(e);
            }
        }
    }

    /**
     * @return
     */
    public String getPath() {
        return pkg.getPath();
    }

    @Override
    public void close() {
        // don't leak the input zip
        pkg.closeReference();
        super.close();
    }
}

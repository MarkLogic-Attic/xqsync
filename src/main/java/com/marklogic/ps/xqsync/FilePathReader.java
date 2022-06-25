/**
 * Copyright (c) 2008-2022 MarkLogic Corporation. All rights reserved.
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

import java.io.File;
import java.io.IOException;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class FilePathReader extends AbstractReader {

    protected final boolean allowEmptyMetadata;

    /**
     * @param configuration
     */
    public FilePathReader(Configuration configuration) throws SyncException {
        // superclass takes care of some configuration
        super(configuration);
        allowEmptyMetadata = configuration.isAllowEmptyMetadata();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.AbstractReader#read(java.lang.String,
     * com.marklogic.ps.xqsync.DocumentInterface)
     */
    @Override
    public void read(String[] uris, DocumentInterface document) throws SyncException {
        try {
            String uri;
            File file;
            for (int i = 0; i < uris.length; i++) {
                uri = uris[i];
                if (uri == null) {
                    throw new SyncException("null path " + i);
                }

                file = new File(uri);

                // read the content: must work for bin or xml, so use bytes
                document.setContent(i, new java.io.FileInputStream(file));

                // read the metadata
                File metaFile = XQSyncDocument.getMetadataFile(file);
                if (!metaFile.exists()) {
                    if (allowEmptyMetadata) {
                        XQSyncDocumentMetadata metadata = new XQSyncDocumentMetadata();
                        document.setMetadata(i, metadata);
                    } else {
                        throw new SyncException("no metadata for " + uri);
                    }
                }
                document.setMetadata(i, new java.io.FileReader(metaFile));
            }
        } catch (IOException e) {
            throw new SyncException(e);
        }
    }

}

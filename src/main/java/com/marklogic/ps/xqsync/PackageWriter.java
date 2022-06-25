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

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class PackageWriter extends AbstractWriter implements Closeable {

    final OutputPackage pkg;

    /**
     * @param configuration
     * @param pkg
     */
    public PackageWriter(Configuration configuration, OutputPackage pkg) {
        // superclass takes care of some configuration
        super(configuration);
        this.pkg = pkg;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.marklogic.ps.xqsync.WriterInterface#write(java.lang.String,
     * byte[], com.marklogic.ps.xqsync.XQSyncDocumentMetadata)
     */
    public int write(String uri, byte[] bytes, XQSyncDocumentMetadata metadata) throws SyncException {
        if (null == pkg) {
            throw new SyncException("null output package");
        }
        if (null == bytes) {
            throw new SyncException("null output bytes");
        }
        if (null == metadata) {
            throw new SyncException("null output metadata");
        }
        try {
            // bytes from one document should not overflow int
            return (int) pkg.write(uri, bytes, metadata);
        } catch (IOException e) {
            throw new SyncException(e);
        }
        // NB - caller must flush() the package
    }

    /**
     *
     */
    public void close() {
        if (null != pkg) {
            pkg.close();
        }
    }

}

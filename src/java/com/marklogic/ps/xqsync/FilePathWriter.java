/**
 * Copyright (c) 2008-2009 Mark Logic Corporation. All rights reserved.
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
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class FilePathWriter extends AbstractWriter {

    protected String root;

    /**
     * @param _configuration
     * @throws SyncException
     */
    public FilePathWriter(Configuration _configuration)
            throws SyncException {
        super(_configuration);
        
        root = _configuration.getOutputPath();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.WriterInterface#write(java.lang.String,
     * byte[], com.marklogic.ps.xqsync.XQSyncDocumentMetadata)
     */
    public int write(String uri, byte[] bytes,
            XQSyncDocumentMetadata _metadata) throws SyncException {
        try {
            File outputFile = new File(root,uri);
            File parent = outputFile.getParentFile();
            if (null == parent) {
                throw new FatalException("no parent for " + uri);
            }
            if (!parent.exists()) {
                parent.mkdirs();
            }

            if (!parent.isDirectory()) {
                throw new SyncException("parent is not a directory: "
                        + parent.getCanonicalPath());
            }

            if (!parent.canWrite()) {
                throw new SyncException(
                        "cannot write to parent directory: "
                                + parent.getCanonicalPath());
            }

            FileOutputStream fos = new FileOutputStream(outputFile);
            fos.write(bytes);
            fos.flush();
            fos.close();

            String metadataPath = XQSyncDocument
                    .getMetadataPath(outputFile);
            FileOutputStream mfos = new FileOutputStream(metadataPath);
            byte[] metaBytes = _metadata.toXML().getBytes();
            mfos.write(metaBytes);
            mfos.flush();
            mfos.close();

            return bytes.length + metaBytes.length;
        } catch (IOException e) {
            throw new SyncException(e);
        }
    }

}

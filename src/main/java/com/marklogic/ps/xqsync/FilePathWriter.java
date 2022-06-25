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
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class FilePathWriter extends AbstractWriter {

    protected final String root;

    /**
     * @param configuration
     */
    public FilePathWriter(Configuration configuration) {
        super(configuration);
        root = configuration.getOutputPath();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.WriterInterface#write(java.lang.String, byte[], com.marklogic.ps.xqsync.XQSyncDocumentMetadata)
     */
    public int write(String uri, byte[] bytes, XQSyncDocumentMetadata metadata) throws SyncException {
        try {
            File outputFile = new File(root, uri);
            write(bytes, outputFile);

            int metaBytesLength = writeMetadataFile(metadata, outputFile);
            return bytes.length + metaBytesLength;
        } catch (IOException e) {
            throw new SyncException(e);
        }
    }

    protected int writeMetadataFile(XQSyncDocumentMetadata metadata, File outputFile) throws IOException, SyncException {
        String metadataFilePath = XQSyncDocument.getMetadataPath(outputFile);
        byte[] metaBytes = metadata.toXML().getBytes();
        write(metaBytes, metadataFilePath);
        return metaBytes.length;
    }

    protected void write(byte[] bytes, String filePath) throws IOException, SyncException {
        File file = new File(filePath);
        write(bytes, file);
    }

    protected void write(byte[] bytes, File outputFile) throws IOException, SyncException {
        File parent = outputFile.getParentFile();
        if (null == parent) {
            throw new FatalException("no parent for " + outputFile.getCanonicalPath());
        }
        if (!parent.exists()) {
            parent.mkdirs();
        }
        if (!parent.isDirectory()) {
            throw new SyncException("parent is not a directory: " + parent.getCanonicalPath());
        }
        if (!parent.canWrite()) {
            throw new SyncException("cannot write to parent directory: " + parent.getCanonicalPath());
        }
        try (FileOutputStream mfos = new FileOutputStream(outputFile)) {
            mfos.write(bytes);
            mfos.flush();
        }
    }

}

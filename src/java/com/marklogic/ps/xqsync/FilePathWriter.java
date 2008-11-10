/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
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

    /**
     * @param _configuration
     * @throws SyncException
     */
    public FilePathWriter(Configuration _configuration)
            throws SyncException {
        super(_configuration);
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
            File outputFile = new File(uri);
            File parent = outputFile.getParentFile();
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

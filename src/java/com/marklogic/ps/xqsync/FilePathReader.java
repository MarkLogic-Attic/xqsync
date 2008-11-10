/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

import java.io.File;
import java.io.IOException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class FilePathReader extends AbstractReader {

    protected boolean allowEmptyMetadata;

    /**
     * @param _configuration
     * @throws SyncException
     */
    public FilePathReader(Configuration _configuration)
            throws SyncException {
        // superclass takes care of some configuration
        super(_configuration);

        allowEmptyMetadata = configuration.isAllowEmptyMetadata();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.AbstractReader#read(java.lang.String,
     * com.marklogic.ps.xqsync.DocumentInterface)
     */
    @Override
    public void read(String _uri, DocumentInterface _document)
            throws SyncException {
        File file = new File(_uri);

        try {
            // read the content: must work for bin or xml, so use bytes
            _document.setContent(new java.io.FileInputStream(file));

            // read the metadata
            File metaFile = XQSyncDocument.getMetadataFile(file);
            if (!metaFile.exists()) {
                if (allowEmptyMetadata) {
                    XQSyncDocumentMetadata metadata = new XQSyncDocumentMetadata();
                    _document.setMetadata(metadata);
                } else {
                    throw new SyncException("no metadata for " + _uri);
                }
            }
            _document.setMetadata(new java.io.FileReader(metaFile));
        } catch (IOException e) {
            throw new SyncException(e);
        }
    }

}

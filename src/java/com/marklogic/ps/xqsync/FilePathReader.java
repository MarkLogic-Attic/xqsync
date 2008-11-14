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
    public void read(String[] _uris, DocumentInterface _document)
            throws SyncException {
        try {
            String uri;
            File file;
            for (int i = 0; i < _uris.length; i++) {
                uri = _uris[i];
                if (uri == null) {
                    throw new SyncException("null path " + i);
                }

                file = new File(uri);

                // read the content: must work for bin or xml, so use bytes
                _document
                        .setContent(i, new java.io.FileInputStream(file));

                // read the metadata
                File metaFile = XQSyncDocument.getMetadataFile(file);
                if (!metaFile.exists()) {
                    if (allowEmptyMetadata) {
                        XQSyncDocumentMetadata metadata = new XQSyncDocumentMetadata();
                        _document.setMetadata(i, metadata);
                    } else {
                        throw new SyncException("no metadata for " + uri);
                    }
                }
                _document
                        .setMetadata(i, new java.io.FileReader(metaFile));
            }
        } catch (IOException e) {
            throw new SyncException(e);
        }
    }

}

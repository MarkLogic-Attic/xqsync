/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
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
    public void read(String _uri, DocumentInterface _document)
            throws SyncException {

        if (_uri == null) {
            throw new SyncException("null path");
        }
        if (pkg == null) {
            throw new SyncException("null input package");
        }

        try {
            // read the content: must work for bin or xml, so use bytes
            _document.setContent(pkg.getContent(_uri));

            // read the metadata
            MetadataInterface metadata = pkg.getMetadataEntry(_uri);
            _document.setMetadata(metadata);
        } catch (IOException e) {
            throw new SyncException(e);
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

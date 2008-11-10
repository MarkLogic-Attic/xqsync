/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;

import java.io.IOException;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
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
    public int write(String uri, byte[] bytes,
            XQSyncDocumentMetadata _metadata) throws SyncException {
        if (null == pkg) {
            throw new SyncException("null output package");
        }
        try {
            // bytes from one document should not overflow int
            return (int) pkg.write(uri, bytes, _metadata);
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

/**
 * Copyright (c) 2008-2009 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.xqsync;


/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class PackageTaskFactory extends TaskFactory {

    /**
     * @param _config
     * @param _inputPackage 
     * @throws SyncException
     */
    public PackageTaskFactory(Configuration _config, InputPackage _inputPackage) throws SyncException {
        super(_config);
        inputPackage = _inputPackage;
    }

    protected InputPackage inputPackage;

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.TaskFactory#getReader()
     */
    @Override
    public ReaderInterface getReader() throws SyncException {
        ReaderInterface reader = new PackageReader(configuration);
        ((PackageReader) reader).setPackage(inputPackage);
        return reader;
    }

}

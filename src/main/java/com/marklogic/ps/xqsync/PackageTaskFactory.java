/**
 * Copyright (c) 2008-2012 MarkLogic Corporation. All rights reserved.
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

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class PackageTaskFactory extends TaskFactory {

    /**
     * @param _config
     * @param _monitor
     * @param _inputPackage
     * @throws SyncException
     */
    public PackageTaskFactory(Configuration _config, Monitor _monitor,
            InputPackage _inputPackage) throws SyncException {
        super(_config, _monitor);
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

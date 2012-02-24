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

import java.io.InputStream;
import java.io.Reader;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public interface DocumentInterface {

    /**
     * @param _index
     * @param _bytes
     * @throws SyncException
     */
    public void setContent(int _index, byte[] _bytes)
            throws SyncException;

    /**
     * @param _index
     * @param _metadata
     * @throws SyncException
     */
    public void setMetadata(int _index, Reader _metadata)
            throws SyncException;

    /**
     * @throws SyncException
     * 
     */
    public int sync() throws SyncException;

    /**
     * @param _index
     * @param _metadata
     */
    public void setMetadata(int _index, MetadataInterface _metadata);

    /**
     * @return
     */
    public MetadataInterface newMetadata();

    /**
     * @param _index
     * @param _is
     * @throws SyncException
     */
    public void setContent(int _index, InputStream _is)
            throws SyncException;

    /**
     * @param _index
     * @param _reader
     * @throws SyncException
     */
    public void setContent(int _index, Reader _reader)
            throws SyncException;

    /**
     * @param _index
     * @return
     */
    public String getOutputUri(int _index);

    /**
     * @param _index
     */
    public void clearPermissions(int _index);

    /**
     * @param _index
     */
    public void clearProperties(int _index);

}

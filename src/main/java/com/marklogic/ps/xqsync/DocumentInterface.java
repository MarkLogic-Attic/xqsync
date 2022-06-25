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

import java.io.InputStream;
import java.io.Reader;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public interface DocumentInterface {

    /**
     * @param index
     * @param bytes
     * @throws SyncException
     */
    void setContent(int index, byte[] bytes) throws SyncException;

    /**
     * @param index
     * @param metadata
     * @throws SyncException
     */
    void setMetadata(int index, Reader metadata) throws SyncException;

    /**
     * @throws SyncException
     * 
     */
    int sync() throws SyncException;

    /**
     * @param index
     * @param metadata
     */
    void setMetadata(int index, MetadataInterface metadata);

    /**
     * @return
     */
    MetadataInterface newMetadata();

    /**
     * @param index
     * @param is
     * @throws SyncException
     */
    void setContent(int index, InputStream is) throws SyncException;

    /**
     * @param index
     * @param reader
     * @throws SyncException
     */
    void setContent(int index, Reader reader) throws SyncException;

    /**
     * @param index
     * @return
     */
    String getOutputUri(int index);

    /**
     * @param index
     */
    void clearPermissions(int index);

    /**
     * @param index
     */
    void clearProperties(int index);

}

/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c) 2008-2012 Mark Logic Corporation. All rights reserved.
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

import com.marklogic.xcc.types.XSInteger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 */
public interface MetadataInterface {

    /**
     * @param _format
     */
    void setFormat(String _format);

    /**
     * @param _uri
     */
    void addCollection(String _uri);

    /**
     * @param _capability
     * @param _role
     */
    void addPermission(String _capability, String _role);

    /**
     * @param _quality
     */
    void setQuality(XSInteger _quality);

    /**
     * @return
     */
    boolean isBinary();

    /**
     * @param _xml
     */
    void setProperties(String _xml);

    /**
     * @return
     */
    String getFormatName();

    /**
     * 
     */
    void clearPermissions();

    /**
     * 
     */
    void clearProperties();

    /**
     * Set the hash value for this document
     */
    void setHashValue(String hashValue);

    /**
     * @return the hash value for this document
     */
    String getHashValue();
    
  
}

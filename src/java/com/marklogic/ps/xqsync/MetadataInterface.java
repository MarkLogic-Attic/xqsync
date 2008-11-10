/**
 * Copyright (c) 2008 Mark Logic Corporation. All rights reserved.
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

}

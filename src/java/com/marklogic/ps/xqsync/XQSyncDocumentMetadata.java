/*
 * Copyright 2005 Mark Logic Corporation. All rights reserved.
 *
 */
package com.marklogic.ps.xqsync;

import java.io.Reader;
import java.util.Collection;
import java.util.Vector;

import com.marklogic.xdmp.XDMPDocInsertStream;
import com.marklogic.xdmp.XDMPPermission;
import com.thoughtworks.xstream.XStream;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 *  
 */
public class XQSyncDocumentMetadata {
    private int format = XDMPDocInsertStream.XDMP_DOC_FORMAT_XML;

    Vector collectionsVector = new Vector();

    Vector permissionsVector = new Vector();

    private int quality = 0;

    private String properties = null;

    private static XStream xstream = new XStream();

    /**
     * @param reader
     */
    public static XQSyncDocumentMetadata fromXML(Reader reader) {
        return (XQSyncDocumentMetadata) xstream.fromXML(reader);
    }

    /**
     * @return
     */
    public boolean isBinary() {
        return format == XDMPDocInsertStream.XDMP_DOC_FORMAT_BINARY;
    }

    /**
     * @param _format
     */
    public void setFormat(int _format) {
        format = _format;
    }

    /**
     * @param _collection
     */
    public void addCollection(String _collection) {
        collectionsVector.add(_collection);
    }

    /**
     * @param _permission
     */
    public void addPermission(XDMPPermission _permission) {
        permissionsVector.add(_permission);
    }

    /**
     * @param _quality
     */
    public void setQuality(int _quality) {
        quality = _quality;
    }

    /**
     * @param _properties
     */
    public void setProperties(String _properties) {
        properties = _properties;
    }

    /**
     * @return
     */
    public String[] getCollections() {
        return (String[]) collectionsVector.toArray(new String[0]);
    }

    /**
     * @return
     */
    public String getProperties() {
        return properties;
    }

    /**
     * @param roles
     */
    public void addPermissions(Collection roles) {
        permissionsVector.addAll(roles);
    }

    /**
     * @return
     */
    public XDMPPermission[] getPermissions() {
        return (XDMPPermission[]) permissionsVector
                .toArray(new XDMPPermission[0]);
    }

    /**
     * @return
     */
    public int getQuality() {
        return quality;
    }

    /**
     * @return
     */
    public int getFormat() {
        return format;
    }

    /**
     * @return
     */
    public String toXML() {
        return xstream.toXML(this);
    }

    /**
     * 
     */
    public void clearPermissions() {
        collectionsVector.clear();
    }

    /**
     * 
     */
    public void clearProperties() {
        properties = null;
    }

}
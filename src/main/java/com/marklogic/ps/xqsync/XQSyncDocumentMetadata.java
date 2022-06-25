/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c)2004-2022 MarkLogic Corporation
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

import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.types.XSInteger;
import com.thoughtworks.xstream.XStream;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class XQSyncDocumentMetadata implements MetadataInterface {

    static final XStream xstream = new XStream();
    DocumentFormat format = DocumentFormat.XML;
    final List<String> collectionsList = new ArrayList<>();
    final List<ContentPermission> permissionsList = new ArrayList<>();
    int quality = 0;
    String properties = null;
    protected String hashValue = null;

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
        return DocumentFormat.BINARY.toString().equals(format.toString());
    }

    /**
     * @param format
     */
    public void setFormat(DocumentFormat format) {
        this.format = format;
    }

    /**
     * @param collection
     */
    public void addCollection(String collection) {
        collectionsList.add(collection);
    }

    /**
     * @param permission
     */
    public void addPermission(ContentPermission permission) {
        permissionsList.add(permission);
    }

    /**
     * @param quality
     */
    public void setQuality(int quality) {
        this.quality = quality;
    }

    /**
     * @param properties
     */
    public void setProperties(String properties) {
        this.properties = properties;
    }

    /**
     * @return
     */
    public String[] getCollections() {
        return collectionsList.toArray(new String[0]);
    }

    /**
     * @return
     */
    public String getProperties() {
        return properties;
    }

    /**
     * @param permissions
     */
    public void addPermissions(Collection<ContentPermission> permissions) {
        if (permissions == null) {
            return;
        }
        permissionsList.addAll(permissions);
    }

    /**
     * @return
     */
    public ContentPermission[] getPermissions() {
        if (permissionsList.isEmpty()) {
            return new ContentPermission[0];
        }
        return permissionsList.toArray(new ContentPermission[0]);
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
    public DocumentFormat getFormat() {
        return format;
    }

    /**
     * @return
     */
    public String toXML() {
        // note that this will escape the properties... do we care? no.
        return xstream.toXML(this);
    }

    /**
     *
     */
    public void clearPermissions() {
        permissionsList.clear();
    }

    /**
     *
     */
    public void clearProperties() {
        properties = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.MetadataInterface#getFormatName()
     */
    public String getFormatName() {
        return format.toString();
    }

    /**
     * @param format
     */
    public void setFormat(String format) {
        if (DocumentFormat.XML.toString().equals(format)
                || "element".equals(format) || "comment".equals(format)
                || "processing-instruction".equals(format)) {
            setFormat(DocumentFormat.XML);
            return;
        }

        if (DocumentFormat.TEXT.toString().equals(format) || "text".equals(format)) {
            setFormat(DocumentFormat.TEXT);
            return;
        }

        // default
        setFormat(DocumentFormat.BINARY);
    }

    /**
     * @param capability
     * @param role
     */
    public void addPermission(String capability, String role) {
        ContentCapability contentCapability;
        if (ContentPermission.UPDATE.toString().equals(capability)) {
            contentCapability = ContentPermission.UPDATE;
        } else if (ContentPermission.INSERT.toString().equals(capability)) {
            contentCapability = ContentPermission.INSERT;
        } else if (ContentPermission.EXECUTE.toString().equals(capability)) {
            contentCapability = ContentPermission.EXECUTE;
        } else if (ContentPermission.READ.toString().equals(capability)) {
            contentCapability = ContentPermission.READ;
        } else {
            throw new UnimplementedFeatureException("unknown capability: " + capability);
        }
        addPermission(new ContentPermission(contentCapability, role));
    }

    /**
     * @param integer
     */
    public void setQuality(XSInteger integer) {
        setQuality(integer.asPrimitiveInt());
    }

    /**
     * @return
     */
    public boolean isText() {
        return DocumentFormat.TEXT.toString().equals(format.toString());
    }

    /**
     * @return
     */
    public boolean isXml() {
        return DocumentFormat.XML.toString().equals(format.toString());
    }

    /**
     * @param collections
     */
    public void addCollections(String[] collections) {
        if (null == collections || 1 > collections.length) {
            return;
        }
        for (String collection : collections) {
            addCollection(collection);
        }
    }

    /**
     * Set the hash value for this document
     */
    public void setHashValue(String hashValue) {
        this.hashValue = hashValue;
    }

    /**
     * @return the hash value for this document
     */
    public String getHashValue() {
        return hashValue;
    }
}

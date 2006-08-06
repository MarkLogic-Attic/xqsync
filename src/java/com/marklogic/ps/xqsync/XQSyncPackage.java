/*
 * Copyright (c)2004-2006 Mark Logic Corporation
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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import com.marklogic.ps.AbstractLoggableClass;
import com.marklogic.ps.Utilities;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class XQSyncPackage extends AbstractLoggableClass {

    private String packagePath;

    private ZipOutputStream outputZip;

    private ZipFile inputZip;
    
    private Object outputMutex = new Object();

    // per-zip output byte count
    private long outputBytes = 0;

    /**
     * @param _stream
     */
    public XQSyncPackage(OutputStream _stream) {
        outputZip = new ZipOutputStream(_stream);
    }

    /**
     * @param _path
     * @throws IOException
     */
    public XQSyncPackage(String _path) throws IOException {
        inputZip = new ZipFile(_path);
    }

    /**
     * @return Returns the package's filesystem path.
     */
    public String getPath() {
        return packagePath;
    }

    /**
     * @param outputPath
     * @param bytes
     * @param metadata
     * @throws IOException
     */
    public void write(String outputPath, byte[] bytes,
            XQSyncDocumentMetadata metadata) throws IOException {
        // TODO use size metrics to automatically manage multiple zip archives,
        // to avoid 32-bit limits in java.util.zip
        /*
         * An exception-based mechanism would be tricky, here:
         * we definitely want the content and the meta entries to stay
         * in the same zipfile.
         */
        byte[] metaBytes = metadata.toXML().getBytes();
        long total = bytes.length + metaBytes.length;
        synchronized (outputMutex) {
            if (outputBytes + total > Integer.MAX_VALUE) {
                logger.fine("package bytes would exceed 32-bit limit");
                flush();
                
            }
            ZipEntry entry = new ZipEntry(outputPath);
            outputZip.putNextEntry(entry);
            outputZip.write(bytes);
            outputZip.closeEntry();

            String metadataPath = XQSyncDocument
                    .getMetadataPath(outputPath);
            entry = new ZipEntry(metadataPath);
            outputZip.putNextEntry(entry);
            outputZip.write(metaBytes);
            outputZip.closeEntry();
            
            outputBytes += total;
        }
    }

    /**
     * @throws IOException
     * 
     */
    public void flush() throws IOException {
        synchronized (outputMutex) {
            outputZip.flush();
        }
    }

    /**
     * @throws IOException
     * 
     */
    public void close() throws IOException {
        synchronized (outputMutex) {
            outputZip.close();
        }
    }

    /**
     * @param _path
     * @return
     * @throws IOException
     */
    public XQSyncDocumentMetadata getMetadataEntry(String _path)
            throws IOException {
        String metadataPath = XQSyncDocument.getMetadataPath(_path);
        ZipEntry metadataEntry = getEntry(metadataPath);

        return XQSyncDocumentMetadata.fromXML(new InputStreamReader(inputZip
                .getInputStream(metadataEntry)));
    }

    /**
     * @param _path
     * @return
     */
    private ZipEntry getEntry(String _path) {
        return inputZip.getEntry(_path);
    }

    /**
     * @param _path
     * @return
     * @throws IOException
     */
    public byte[] getContent(String _path) throws IOException {
        ZipEntry entry = inputZip.getEntry(_path);
        return Utilities.cat(inputZip.getInputStream(entry));
    }

    /**
     * @return
     */
    public List<String> list() {
        Enumeration e = inputZip.entries();
        HashSet<String> documentList = new HashSet<String>();

        String path;
        ZipEntry entry;
        while (e.hasMoreElements()) {
            entry = (ZipEntry) e.nextElement();

            // ignore directories
            if (entry.isDirectory())
                continue;

            path = entry.getName();
            // logger.finest("found " + path);
            // whether it's metadata or not, we add the same path
            if (path.endsWith(XQSyncDocument.METADATA_EXT)) {
                path = path.substring(0, path.length()
                        - XQSyncDocument.METADATA_EXT.length());
            }

            // make sure we don't add duplicates
            if (!documentList.contains(path)) {
                // logger.finest("adding " + path);
                documentList.add(path);
            }
        }

        return new LinkedList<String>(documentList);
    }

}

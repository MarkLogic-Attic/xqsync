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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import com.marklogic.ps.AbstractLoggableClass;
import com.marklogic.ps.Utilities;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class InputPackage extends AbstractLoggableClass {
    // 34464 entries max
    // ref: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4418997
    // (supposed to be closed, but isn't)
    private static final int MAX_ENTRIES = 34464;

    private String packagePath;

    private ZipFile inputZip;

    private File inputFile;

    /**
     * @param _path
     * @throws IOException
     */
    public InputPackage(String _path) throws IOException {
        inputFile = new File(_path);
        inputZip = new ZipFile(inputFile);
    }

    /**
     * @return Returns the package's filesystem path.
     */
    public String getPath() {
        return packagePath;
    }

    /**
     * @param _path
     * @return
     * @throws IOException
     */
    public XQSyncDocumentMetadata getMetadataEntry(String _path)
            throws IOException {
        return XQSyncDocumentMetadata.fromXML(new InputStreamReader(
                getEntryStream(XQSyncDocument.getMetadataPath(_path))));
    }

    /**
     * @param _path
     * @return
     */
    private InputStream getEntryStream(String _path) throws IOException {
        ZipEntry entry = inputZip.getEntry(_path);
        if (entry != null) {
            return inputZip.getInputStream(entry);
        }

        int size = inputZip.size();
        if (size >= MAX_ENTRIES) {
            logger.warning("too many entries in input-package: " + size
                    + " >= " + MAX_ENTRIES + " (" + _path + ")");
            // *slow* work around for the dumb bug
            ZipInputStream zis = new ZipInputStream(new FileInputStream(
                    inputFile));

            while ((entry = zis.getNextEntry()) != null
                    && !entry.getName().equals(_path)) {
                // loop until the path matches, or we hit the end
            }
            return zis;
        }

        // otherwise there's no hope: something went very wrong
        throw new IOException("entry " + _path + " not found");
    }

    /**
     * @param _path
     * @return
     * @throws IOException
     */
    public byte[] getContent(String _path) throws IOException {
        return Utilities.cat(getEntryStream(_path));
    }

    /**
     * @return
     */
    public List<String> list() throws IOException {
        int size = inputZip.size();
        logger.fine("expecting " + size + " entries");

        ZipEntry entry;
        long entries = 0;
        HashSet<String> documentList = new HashSet<String>();

        // there doesn't seem to be anything we can do about this
        if (size < MAX_ENTRIES) {
            Enumeration e = inputZip.entries();

            while (e.hasMoreElements()) {
                entry = (ZipEntry) e.nextElement();
                entries += addEntry(entry, documentList);
            }
        } else {
            logger.warning("too many entries in input-package: " + size
                    + " >= " + MAX_ENTRIES);

            ZipInputStream zis = new ZipInputStream(new FileInputStream(
                    inputFile));

            while ((entry = zis.getNextEntry()) != null) {
                entries += addEntry(entry, documentList);
            }
        }
        logger.fine("listed " + documentList.size() + " documents from "
                + entries + " entries");

        return new LinkedList<String>(documentList);
    }

    private int addEntry(ZipEntry entry, HashSet<String> documentList) {
        // ignore directories
        if (entry.isDirectory()) {
            return 0;
        }

        String path = entry.getName();
        logger.finest("found " + path);

        // whether it's metadata or not, we add the same path
        if (path.endsWith(XQSyncDocument.METADATA_EXT)) {
            path = path.substring(0, path.length()
                    - XQSyncDocument.METADATA_EXT.length());
        }

        // make sure we don't add duplicates
        if (documentList.contains(path)) {
            return 0;
        }

        // logger.finest("adding " + path);
        documentList.add(path);
        return 1;
    }

}

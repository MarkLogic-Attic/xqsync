/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c)2004-2009 Mark Logic Corporation
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

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
public class InputPackage {

    protected static SimpleLogger logger;

    protected Configuration configuration;

    // number of entries overflows at 2^16 = 65536
    // ref: http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4828461
    // (supposed to be fixed, but isn't)
    protected static final int MAX_ENTRIES = 65536 - 1;

    protected String packagePath;

    protected ZipFile inputZip;

    protected File inputFile;

    protected volatile int references = 0;

    protected boolean allowEmptyMetadata;

    protected Object referenceMutex = new Object();

    /**
     * @param _path
     * @param _config
     * @throws IOException
     */
    public InputPackage(String _path, Configuration _config)
            throws IOException {
        inputFile = new File(_path);
        inputZip = new ZipFile(inputFile);
        packagePath = inputFile.getCanonicalPath();
        configuration = _config;
        logger = configuration.getLogger();

        allowEmptyMetadata = configuration.isAllowEmptyMetadata();
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
        InputStream entryStream = getEntryStream(XQSyncDocument
                .getMetadataPath(_path));
        if (allowEmptyMetadata && null == entryStream) {
            return new XQSyncDocumentMetadata();
        }
        return XQSyncDocumentMetadata.fromXML(new InputStreamReader(
                entryStream));
    }

    /**
     * @param _path
     * @return
     */
    private InputStream getEntryStream(String _path) throws IOException {
        ZipEntry entry = inputZip.getEntry(_path);
        if (null != entry) {
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

        if (allowEmptyMetadata
                && _path.endsWith(XQSyncDocument.METADATA_EXT)) {
            return null;
        }

        // otherwise there's no hope: something went very wrong
        throw new IOException("entry " + _path + " not found in "
                + inputZip.getName());
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
        if (null == inputZip) {
            throw new NullPointerException("no input zip file");
        }
        int size = inputZip.size();
        logger.fine("expecting " + size + " entries");
        if (0 == size) {
            logger.warning("0 entries found in " + packagePath);
        }

        ZipEntry entry;
        long entries = 0;
        HashSet<String> documentList = new HashSet<String>();

        // there doesn't seem to be anything we can do about this
        if (size < MAX_ENTRIES) {
            Enumeration<? extends ZipEntry> e = inputZip.entries();

            while (e.hasMoreElements()) {
                entry = e.nextElement();
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
        // do *not* simply ignore directories
        // javadoc says: "defined to be one whose name ends with a '/'"
        // but that's a lousy test here - "/" is a legal document uri
        // instead, skip only if zero-length (even that may be too much)
        if (entry.isDirectory() && entry.getSize() == 0) {
            // really there shouldn't be anything like this in a package
            logger.warning("skipping zero-length directory "
                    + entry.getName());
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

    /**
     *
     */
    public void addReference() {
        synchronized (referenceMutex) {
            // logger.info(inputZip.getName() + " (" + references + ")");
            references++;
        }
    }

    /**
     *
     */
    public void closeReference() {
        synchronized (referenceMutex) {
            // if (references < 2) {
            // logger.info(inputZip.getName() + " (" + references + ")");
            // Thread.dumpStack();
            // }
            references--;

            if (0 > references) {
                throw new FatalException("bad reference count for "
                        + inputZip.getName() + " : " + references);
            }

            if (0 != references) {
                return;
            }

            // free the resources for the input zip package
            logger.fine("closing " + inputZip.getName() + " ("
                    + references + ")");
            try {
                inputZip.close();
            } catch (IOException e) {
                // should not happen - log it and proceed
                logger.logException(inputZip.getName(), e);
            }
        }
    }

    /**
     * @return
     */
    public int size() {
        return inputZip.size();
    }

}

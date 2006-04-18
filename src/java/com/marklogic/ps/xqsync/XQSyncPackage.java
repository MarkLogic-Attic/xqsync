/*
 * Copyright 2005 Mark Logic Corporation. All rights reserved.
 *
 */
package com.marklogic.ps.xqsync;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import com.marklogic.ps.Utilities;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 *  
 */
public class XQSyncPackage {

    private String packagePath;

    private ZipOutputStream outPkg;

    private ZipFile inPkg;

    /**
     * @param _stream
     */
    public XQSyncPackage(OutputStream _stream) {
        outPkg = new ZipOutputStream(_stream);
    }

    /**
     * @param _path
     * @throws IOException
     */
    public XQSyncPackage(String _path) throws IOException {
        inPkg = new ZipFile(_path);
    }

    /**
     * @return Returns the package's filesystem path.
     */
    public String getPath() {
        return packagePath;
    }

    /**
     * @param outputPath
     * @param binaryContent
     * @param metadata
     * @throws IOException
     */
    public void write(String outputPath, byte[] binaryContent,
            XQSyncDocumentMetadata metadata) throws IOException {
        synchronized (outPkg) {
            ZipEntry entry = new ZipEntry(outputPath);
            outPkg.putNextEntry(entry);
            outPkg.write(binaryContent);
            outPkg.closeEntry();

            String metadataPath = XQSyncDocument.getMetadataPath(outputPath);
            entry = new ZipEntry(metadataPath);
            outPkg.putNextEntry(entry);
            outPkg.write(metadata.toXML().getBytes());
            outPkg.closeEntry();
        }
    }

    /**
     * @throws IOException
     *  
     */
    public void flush() throws IOException {
        synchronized (outPkg) {
            outPkg.flush();
        }
    }

    /**
     * @throws IOException
     *  
     */
    public void close() throws IOException {
        synchronized (outPkg) {
            outPkg.close();
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

        return XQSyncDocumentMetadata.fromXML(new InputStreamReader(
                inPkg.getInputStream(metadataEntry)));
    }

    /**
     * @param _path
     * @return
     */
    private ZipEntry getEntry(String _path) {
        return inPkg.getEntry(_path);
    }

    /**
     * @param _path
     * @return
     * @throws IOException
     */
    public byte[] getContent(String _path) throws IOException {
        ZipEntry entry = inPkg.getEntry(_path);
        return Utilities.cat(inPkg.getInputStream(entry));
    }

    /**
     * @return
     */
    public List list() {
        Enumeration e = inPkg.entries();
        HashMap documentList = new HashMap();

        String path;
        ZipEntry entry;
        while (e.hasMoreElements()) {            
            entry = (ZipEntry) e.nextElement();
            
            // ignore directories
            if (entry.isDirectory())
                continue;
            
            path = entry.getName();
            //logger.finest("found " + path);
            // whether it's metadata or not, we add the same path
            if (path.endsWith(XQSyncDocument.METADATA_EXT)) {
                path = path.substring(0, path.length()
                        - XQSyncDocument.METADATA_EXT.length());
            }
            
            // make sure we don't add duplicates
            if (!documentList.containsKey(path)) {
                //logger.finest("adding " + path);
                documentList.put(path, null);
            }
        }
        
        return new LinkedList(documentList.keySet());
    }

}
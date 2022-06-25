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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import java.net.URI;
import java.net.URISyntaxException;

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class XQSyncDocument implements DocumentInterface {

    public static final String UTF_8 = "UTF-8";
    public static final String ENCODING = UTF_8;
    public static final String METADATA_EXT = ".metadata";
    public static final String METADATA_REGEX = "^.+\\" + XQSyncDocument.METADATA_EXT + "$";
    protected final byte[][] contentBytes;
    protected final XQSyncDocumentMetadata[] metadata;
    protected SimpleLogger logger = null;
    protected final WriterInterface writer;
    protected final ReaderInterface reader;
    protected final Configuration configuration;
    protected final String[] inputUris;
    protected String[] outputUris;
    protected final boolean copyPermissions;
    protected final boolean copyProperties;

    /**
     * @param uris
     * @param reader
     * @param writer
     * @param configuration
     */
    public XQSyncDocument(String[] uris, ReaderInterface reader, WriterInterface writer, Configuration configuration) {
        inputUris = uris;
        this.reader = reader;
        this.writer = writer;
        this.configuration = configuration;

        logger = configuration.getLogger();

        copyPermissions = configuration.isCopyPermissions();
        copyProperties = configuration.isCopyProperties();

        metadata = new XQSyncDocumentMetadata[inputUris.length];
        contentBytes = new byte[inputUris.length][];

        composeOutputUris();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#setContent(int, byte[])
     */
    public void setContent(int index, byte[] bytes) {
        contentBytes[index] = bytes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.ps.xqsync.DocumentInterface#setMetadata(java.io.Reader)
     */
    public void setMetadata(int index, Reader reader) {
        metadata[index] = XQSyncDocumentMetadata.fromXML(reader);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#newMetadata()
     */
    public MetadataInterface newMetadata() {
        return new XQSyncDocumentMetadata();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#sync()
     */
    public int sync() throws SyncException {
        read();
        return write();
    }

    /**
     * @return
     * @throws SyncException
     */
    private int write() throws SyncException {
        String uri = null;
        int len = 0;
        for (int i = 0; i < outputUris.length; i++) {
            if (null == inputUris[i]) {
                continue;
            }
            uri = outputUris[i];
            if (null == contentBytes[i]) {
                throw new NullPointerException("null content bytes at " + i + " (" + uri + "(");
            }
            if (null == metadata[i]) {
                throw new NullPointerException("null metadata at " + i + " (" + uri + "(");
            }

            // check for and strip BOM
            if (contentBytes[i].length > 2 && !metadata[i].isBinary()
                    && (byte) 0xEF == contentBytes[i][0]
                    && (byte) 0xBB == contentBytes[i][1]
                    && (byte) 0xBF == contentBytes[i][2]) {
                logger.finer("stripping BOM from " + uri);
                byte[] copy = new byte[contentBytes[i].length - 3];
                System.arraycopy(contentBytes[i], 3, copy, 0, copy.length);
                contentBytes[i] = copy;
            }
        }
	      len = writer.write(outputUris, contentBytes, metadata);
        return len;
    }

    /**
     * @throws SyncException
     */
    public void read() throws SyncException {
        if (null != contentBytes[0]) {
            return;
        }

        reader.read(inputUris, this);

        // implement any configuration-mandated changes
        for (int i = 0; i < metadata.length; i++) {
            if (null == inputUris[i]) {
                continue;
            }
            if (null == metadata[i]) {
                throw new NullPointerException(
                        "unexpected empty metadata at " + i + ", "
                                + inputUris[i]);
            }
            metadata[i].addCollections(configuration.getOutputCollections());
            if (!copyPermissions) {
                metadata[i].clearPermissions();
            }
            if (!copyProperties) {
                metadata[i].clearProperties();
            }
        }
    }

    public static File getMetadataFile(File contentFile) throws IOException {
        return new File(getMetadataPath(contentFile));
    }

    /**
     * @param path
     * @return
     */
    public static String getMetadataPath(String path) {
        return path + METADATA_EXT;
    }

    /**
     * @param file
     * @return
     * @throws IOException
     */
    public static String getMetadataPath(File file) throws IOException {
        return getMetadataPath(file.getCanonicalPath());
    }

    private void composeOutputUris() {
        StringBuilder uriPrefix = new StringBuilder();
        if (configuration.getUriPrefix() != null) {
            uriPrefix.append(configuration.getUriPrefix());
        }
        String uriSuffix = configuration.getUriSuffix();
        String prefixStrip = configuration.getUriPrefixStrip();
        String suffixStrip = configuration.getUriSuffixStrip();
        String uri;
        outputUris = new String[inputUris.length];
        for (int i = 0; i < inputUris.length; i++) {
            uri = inputUris[i];
            if (null == uri) {
                continue;
            }

            String outputUri;
            // strip prefix and suffix as needed
            if (null != prefixStrip && uri.startsWith(prefixStrip)) {
                uri = uri.substring(prefixStrip.length() - 1);
            }
            if (null != suffixStrip && uri.endsWith(suffixStrip)) {
                uri = uri.substring(0, uri.length() - suffixStrip.length() - 1);
            }

            // add extra prefix and suffix as needed
            if (null != uriPrefix && !uriPrefix.toString().equals("") && !uriPrefix.toString().endsWith("/") && !uri.startsWith("/")) {
                uriPrefix.append("/");
            }

            String innerUri = uri;
            if (configuration.useRandomOutputUri()) {
                innerUri = 
                    Long.toHexString(System.currentTimeMillis()) +
                    Long.toHexString(hashCode()) +
                    Long.toHexString(innerUri.hashCode());
            } 

            outputUri = (null == uriPrefix ? "" : uriPrefix.toString()) + innerUri + (null == uriSuffix ? "" : uriSuffix);

            // note that some constructors will need to un-escape the inputUri
            if (configuration.encodeOutputUri()) {
                // NTFS: The period (.) cannot be the first character
                // NTFS: Illegal Characters: / \ : * ? " < > |

                // Supposedly, this will encode URI according to URI's specification.
                try {
                    URI uriObj = new URI(null,null, outputUri,null, null);
                    outputUri = uriObj.toString();
                } catch (URISyntaxException e) {
                    logger.logException("invalid uri", e);
                }
            }
            logger.finer("copying " + uri + " to " + outputUri);
            outputUris[i] = outputUri;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#setMetadata(int,
     * com.marklogic.ps.xqsync.MetadataInterface)
     */
    public void setMetadata(int index, MetadataInterface metadata) {
        this.metadata[index] = (XQSyncDocumentMetadata) metadata;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#setContent(int,
     * java.io.InputStream)
     */
    public void setContent(int index, InputStream is) throws SyncException {
        try {
            contentBytes[index] = Utilities.cat(is);
        } catch (IOException e) {
            throw new SyncException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#setContent(int,
     * java.io.Reader)
     */
    public void setContent(int index, Reader reader) throws SyncException {
        try {
            contentBytes[index] = Utilities.cat(reader).getBytes();
        } catch (IOException e) {
            throw new SyncException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#getOutputUri()
     */
    public String getOutputUri(int index) {
        return outputUris[index];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#clearPermissions(int)
     */
    public void clearPermissions(int index) {
        metadata[index].clearPermissions();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#clearProperties(int)
     */
    public void clearProperties(int index) {
        metadata[index].clearProperties();
    }

    /**
     * @param index
     * @return
     */
    public byte[] getContent(int index) {
        return contentBytes[index];
    }

    /**
     * @param index
     * @return
     */
    public XQSyncDocumentMetadata getMetadata(int index) {
        return metadata[index];
    }

}

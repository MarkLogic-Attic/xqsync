/*
 * Copyright (c)2004-2008 Mark Logic Corporation
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

import com.marklogic.ps.SimpleLogger;
import com.marklogic.ps.Utilities;

/**
 * @author Michael Blakeley <michael.blakeley@marklogic.com>
 * 
 */
/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class XQSyncDocument implements DocumentInterface {

    public static final String UTF_8 = "UTF-8";

    public static final String ENCODING = UTF_8;

    public static final String METADATA_EXT = ".metadata";

    public static final String METADATA_REGEX = "^.+\\"
            + XQSyncDocument.METADATA_EXT + "$";

    protected byte[][] contentBytes;

    protected XQSyncDocumentMetadata[] metadata;

    protected SimpleLogger logger = null;

    protected WriterInterface writer;

    protected ReaderInterface reader;

    protected Configuration configuration;

    protected String[] inputUris;

    protected String[] outputUris;

    protected boolean copyPermissions;

    protected boolean copyProperties;

    /**
     * @param _uris
     * @param _reader
     * @param _writer
     * @param _configuration
     */
    public XQSyncDocument(String[] _uris, ReaderInterface _reader,
            WriterInterface _writer, Configuration _configuration) {
        inputUris = _uris;
        reader = _reader;
        writer = _writer;
        configuration = _configuration;

        logger = configuration.getLogger();

        copyPermissions = configuration.isCopyPermissions();
        copyProperties = configuration.isCopyProperties();

        metadata = new XQSyncDocumentMetadata[inputUris.length];
        contentBytes = new byte[inputUris.length][];

        composeOutputUris(false);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#setContent(int, byte[])
     */
    public void setContent(int _index, byte[] _bytes) {
        contentBytes[_index] = _bytes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.ps.xqsync.DocumentInterface#setMetadata(java.io.Reader)
     */
    public void setMetadata(int _index, Reader _reader) {
        metadata[_index] = XQSyncDocumentMetadata.fromXML(_reader);
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
        int len = 0;
        for (int i = 0; i < outputUris.length; i++) {
            if (null == inputUris[i]) {
                continue;
            }
            if (null == contentBytes[i]) {
                throw new NullPointerException("null content bytes at "
                        + i);
            }
            if (null == metadata[i]) {
                throw new NullPointerException("null metadata at " + i);
            }
            // check for and strip BOM
            if (contentBytes[i].length > 2 && !metadata[i].isBinary()
                    && (byte) 0xEF == contentBytes[i][0]
                    && (byte) 0xBB == contentBytes[i][1]
                    && (byte) 0xBF == contentBytes[i][2]) {
                logger.finer("stripping BOM from " + outputUris[i]);
                byte[] copy = new byte[contentBytes[i].length - 3];
                System
                        .arraycopy(contentBytes[i], 3, copy, 0,
                                copy.length);
                contentBytes[i] = copy;
            }
            len += writer.write(outputUris[i], contentBytes[i],
                    metadata[i]);
        }
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
            metadata[i].addCollections(configuration
                    .getOutputCollections());
            if (!copyPermissions) {
                metadata[i].clearPermissions();
            }
            if (!copyProperties) {
                metadata[i].clearProperties();
            }
        }
    }

    public static File getMetadataFile(File contentFile)
            throws IOException {
        return new File(getMetadataPath(contentFile));
    }

    /**
     * @param _path
     * @return
     */
    public static String getMetadataPath(String _path) {
        return _path + METADATA_EXT;
    }

    /**
     * @param _path
     * @return
     * @throws IOException
     */
    public static String getMetadataPath(File _file) throws IOException {
        return _file.getCanonicalPath() + METADATA_EXT;
    }

    private void composeOutputUris(boolean _isEscaped) {
        String outputPathPrefix = configuration.getUriPrefix();
        String uri;
        outputUris = new String[inputUris.length];
        for (int i = 0; i < inputUris.length; i++) {
            uri = inputUris[i];
            if (null != outputPathPrefix && !outputPathPrefix.equals("")
                    && !outputPathPrefix.endsWith("/")
                    && !uri.startsWith("/")) {
                outputPathPrefix += "/";
            }

            String outputUri = (null == outputPathPrefix ? ""
                    : outputPathPrefix)
                    + uri;
            // TODO optionally escape outputUri
            // note that some constructors will need to un-escape the inputUri
            if (_isEscaped) {
                // NTFS: The period (.) cannot be the first character
                // NTFS: Illegal Characters: / \ : * ? " < > |
                // TODO note that this is a dummy at present.
                // it's unclear when and what needs to be escaped.
                // outputUri = URLEncoder.encode(outputUri, ENCODING);
                throw new FatalException("UNIMPLEMENTED");
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
    public void setMetadata(int _index, MetadataInterface _metadata) {
        metadata[_index] = (XQSyncDocumentMetadata) _metadata;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#setContent(int,
     * java.io.InputStream)
     */
    public void setContent(int _index, InputStream _is)
            throws SyncException {
        try {
            contentBytes[_index] = Utilities.cat(_is);
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
    public void setContent(int _index, Reader _reader)
            throws SyncException {
        try {
            contentBytes[_index] = Utilities.cat(_reader).getBytes();
        } catch (IOException e) {
            throw new SyncException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#getOutputUri()
     */
    public String getOutputUri(int _index) {
        return outputUris[_index];
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#clearPermissions(int)
     */
    public void clearPermissions(int _index) {
        metadata[_index].clearPermissions();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#clearProperties(int)
     */
    public void clearProperties(int _index) {
        metadata[_index].clearProperties();
    }

    /**
     * @param _index
     * @return
     */
    public byte[] getContent(int _index) {
        return contentBytes[_index];
    }

    /**
     * @param _index
     * @return
     */
    public XQSyncDocumentMetadata getMetadata(int _index) {
        return metadata[_index];
    }

}

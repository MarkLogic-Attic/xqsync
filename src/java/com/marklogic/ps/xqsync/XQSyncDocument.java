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
public class XQSyncDocument implements DocumentInterface {

    public static final String UTF_8 = "UTF-8";

    public static final String ENCODING = UTF_8;

    public static final String METADATA_EXT = ".metadata";

    public static final String METADATA_REGEX = "^.+\\"
            + XQSyncDocument.METADATA_EXT + "$";

    protected byte[] contentBytes;

    protected XQSyncDocumentMetadata metadata;

    protected SimpleLogger logger = null;

    protected WriterInterface writer;

    protected ReaderInterface reader;

    protected Configuration configuration;

    protected String inputUri;

    protected String outputUri;

    protected boolean copyPermissions;

    protected boolean copyProperties;

    /**
     * @param _uri
     * @param _reader
     * @param _writer
     * @param _configuration
     */
    public XQSyncDocument(String _uri, ReaderInterface _reader,
            WriterInterface _writer, Configuration _configuration) {
        inputUri = _uri;
        reader = _reader;
        writer = _writer;
        configuration = _configuration;

        logger = configuration.getLogger();

        copyPermissions = configuration.isCopyPermissions();
        copyProperties = configuration.isCopyProperties();
        
        outputUri = composeOutputUri(false);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#setContent(byte[])
     */
    public void setContent(byte[] _bytes) {
        contentBytes = _bytes;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.ps.xqsync.DocumentInterface#setMetadata(java.io.Reader)
     */
    public void setMetadata(Reader _reader) {
        metadata = XQSyncDocumentMetadata.fromXML(_reader);
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
        return writer.write(outputUri, contentBytes, metadata);
    }

    /**
     * @throws SyncException
     */
    public void read() throws SyncException {
        if (contentBytes != null) {
            return;
        }
        
        reader.read(inputUri, this);

        // implement any configuration-mandated changes
        metadata.addCollections(configuration.getOutputCollections());
        if (!copyPermissions) {
            clearPermissions();
        }
        if (!copyProperties) {
            clearProperties();
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

    private String composeOutputUri(boolean _isEscaped) {
        String outputPathPrefix = configuration.getUriPrefix();

        if (null != outputPathPrefix && !outputPathPrefix.equals("")
                && !outputPathPrefix.endsWith("/")
                && !inputUri.startsWith("/")) {
            outputPathPrefix += "/";
        }

        String outputUri = (null == outputPathPrefix ? ""
                : outputPathPrefix)
                + inputUri;
        // TODO optionally escape outputUri
        // note that some constructors will need to un-escape the inputUri
        if (_isEscaped) {
            // NTFS: The period (.) cannot be the first character
            // NTFS: Illegal Characters: / \ : * ? " < > |
            // TODO note that this is a dummy at present.
            // it's unclear when and what needs to be escaped.
            // outputUri = URLEncoder.encode(outputUri, ENCODING);
        }
        logger.finer("copying " + inputUri + " to " + outputUri);
        return outputUri;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.ps.xqsync.DocumentInterface#setMetadata(com.marklogic.ps
     * .xqsync.MetadataInterface)
     */
    public void setMetadata(MetadataInterface _metadata) {
        metadata = (XQSyncDocumentMetadata) _metadata;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.marklogic.ps.xqsync.DocumentInterface#setContent(java.io.InputStream)
     */
    public void setContent(InputStream _is) throws SyncException {
        try {
            contentBytes = Utilities.cat(_is);
        } catch (IOException e) {
            throw new SyncException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#setContent(java.io.Reader)
     */
    public void setContent(Reader _reader) throws SyncException {
        try {
            contentBytes = Utilities.cat(_reader).getBytes();
        } catch (IOException e) {
            throw new SyncException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.DocumentInterface#getOutputUri()
     */
    public String getOutputUri() {
        return outputUri;
    }

    /* (non-Javadoc)
     * @see com.marklogic.ps.xqsync.DocumentInterface#clearPermissions()
     */
    public void clearPermissions() {
        metadata.clearPermissions();
    }

    /* (non-Javadoc)
     * @see com.marklogic.ps.xqsync.DocumentInterface#clearProperties()
     */
    public void clearProperties() {
        metadata.clearProperties();
    }

    /**
     * @return
     */
    public byte[] getContent() {
        return contentBytes;
    }

    /**
     * @return
     */
    public XQSyncDocumentMetadata getMetadata() {
        return metadata;
    }

}

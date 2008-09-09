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
package com.marklogic.ps;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.List;

/**
 * @author mike.blakeley@marklogic.com
 *
 */
public class Utilities {

    private static final int BUFFER_SIZE = 32 * 1024;

    /**
     * @param _path
     * @return
     */
    public static String getPathExtension(String _path) {
        return _path.replaceFirst(".*\\.([^\\.]+)$", "$1");
    }

    public static String join(List<String> _items, String _delim) {
        return join(_items.toArray(), _delim);
    }

    public static String join(Object[] _items, String _delim) {
        String rval = "";
        for (int i = 0; i < _items.length; i++)
            if (i == 0)
                rval = "" + _items[0];
            else
                rval += _delim + _items[i];
        return rval;
    }

    /**
     * @param _items
     * @param _delim
     * @return
     */
    public static String join(String[] _items, String _delim) {
        if (null == _items)
            return null;

        String rval = "";
        for (int i = 0; i < _items.length; i++)
            if (i == 0)
                rval = _items[0];
            else
                rval += _delim + _items[i];
        return rval;
    }

    public static String escapeXml(String _in) {
        if (_in == null)
            return "";
        return _in.replaceAll("&", "&amp;").replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;");
    }

    public static long copy(InputStream _in, OutputStream _out)
            throws IOException {
        if (_in == null)
            throw new IOException("null InputStream");
        if (_out == null)
            throw new IOException("null OutputStream");

        long totalBytes = 0;
        int len = 0;
        byte[] buf = new byte[BUFFER_SIZE];
        int available = _in.available();
        // System.err.println("DEBUG: " + _in + ": available " + available);
        while ((len = _in.read(buf, 0, BUFFER_SIZE)) > -1) {
            _out.write(buf, 0, len);
            _out.flush();
            totalBytes += len;
            // System.err.println("DEBUG: " + _out + ": wrote " + len);
        }
        // System.err.println("DEBUG: " + _in + ": last read " + len);

        // caller MUST close the stream for us

        // check to see if we copied enough data
        if (available > totalBytes)
            throw new IOException("expected at least " + available
                    + " Bytes, copied only " + totalBytes);

        return totalBytes;
    }

    /**
     * @param _in
     * @param _out
     * @throws IOException
     */
    public static void copy(File _in, File _out) throws IOException {
        InputStream in = new FileInputStream(_in);
        OutputStream out = new FileOutputStream(_out);
        copy(in, out);
    }

    public static long copy(Reader _in, OutputStream _out)
            throws IOException {
        if (_in == null)
            throw new IOException("null Reader");
        if (_out == null)
            throw new IOException("null OutputStream");

        OutputStreamWriter writer = new OutputStreamWriter(_out, "UTF-8");
        long len = copy(_in, writer);

        // caller MUST close the stream for us
        _out.flush();
        return len;
    }

    /**
     * @param _in
     * @param _out
     * @throws IOException
     */
    public static long copy(Reader _in, Writer _out) throws IOException {
        if (_in == null)
            throw new IOException("null Reader");
        if (_out == null)
            throw new IOException("null Writer");

        long totalChars = 0;
        int len = 0;
        char[] cbuf = new char[BUFFER_SIZE];
        while ((len = _in.read(cbuf, 0, BUFFER_SIZE)) > -1) {
            _out.write(cbuf, 0, len);
            _out.flush();
            totalChars += len;
        }

        // caller MUST close the stream for us

        // check to see if we copied enough data
        if (1 > totalChars)
            throw new IOException("expected at least " + 1
                    + " Chars, copied only " + totalChars);

        return totalChars;
    }

    /**
     * @param inFilePath
     * @param outFilePath
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void copy(String inFilePath, String outFilePath)
            throws FileNotFoundException, IOException {
        copy(new FileInputStream(inFilePath), new FileOutputStream(
                outFilePath));
    }

    public static void deleteFile(File _file) throws IOException {
        if (!_file.exists())
            return;

        boolean success;

        if (!_file.isDirectory()) {
            success = _file.delete();
            if (!success) {
                throw new IOException("error deleting "
                        + _file.getCanonicalPath());
            }
            return;
        }

        // directory, so recurse
        File[] children = _file.listFiles();
        if (children != null) {
            for (int i = 0; i < children.length; i++) {
                // recurse
                deleteFile(children[i]);
            }
        }

        // now this directory should be empty
        if (_file.exists()) {
            _file.delete();
        }
    }

    public static final boolean stringToBoolean(String str) {
        // let the caller decide: should an unset string be true or false?
        return stringToBoolean(str, false);
    }

    public static final boolean stringToBoolean(String str,
            boolean defaultValue) {
        if (str == null)
            return defaultValue;

        String lcStr = str.toLowerCase();
        if (str == "" || str.equals("0") || lcStr.equals("f")
                || lcStr.equals("false") || lcStr.equals("n")
                || lcStr.equals("no"))
            return false;

        return true;
    }

    /**
     * @param outHtmlFileName
     * @throws IOException
     */
    public static void deleteFile(String _path) throws IOException {
        deleteFile(new File(_path));
    }

    public static String buildModulePath(Class<?> _class) {
        return "/" + _class.getName().replace('.', '/') + ".xqy";
    }

    public static String buildModulePath(Package _package, String _name) {
        return "/" + _package.getName().replace('.', '/') + "/" + _name
                + (_name.endsWith(".xqy") ? "" : ".xqy");
    }

    /**
     * @param r
     * @return
     * @throws IOException
     */
    public static String cat(Reader r) throws IOException {
        StringBuilder rv = new StringBuilder();

        int size;
        char[] buf = new char[BUFFER_SIZE];
        while ((size = r.read(buf)) > 0) {
            rv.append(buf, 0, size);
        }
        return rv.toString();
    }

    /**
     * @param contentFile
     * @return
     * @throws IOException
     */
    public static byte[] cat(File contentFile) throws IOException {
        return cat(new FileInputStream(contentFile));
    }

    /**
     * @param is
     * @return
     * @throws IOException
     */
    public static byte[] cat(InputStream is) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
        copy(is, bos);
        return bos.toByteArray();
    }

    public static long getSize(InputStream is) throws IOException {
        long size = 0;
        int b = 0;
        byte[] buf = new byte[BUFFER_SIZE];
        while ((b = is.read(buf)) > 0) {
            size += b;
        }
        return size;
    }

    public static long getSize(Reader r) throws IOException {
        long size = 0;
        int b = 0;
        char[] buf = new char[BUFFER_SIZE];
        while ((b = r.read(buf)) > 0) {
            size += b;
        }
        return size;
    }

}

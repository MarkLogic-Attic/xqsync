/*
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
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class Utilities {

    private static final int BUFFER_SIZE = 32 * 1024;

    /**
     * @param path
     * @return
     */
    public static String getPathExtension(String path) {
        return path.replaceFirst(".*\\.([^\\.]+)$", "$1");
    }

    public static String join(List<String> items, String delim) {
        return join(items.toArray(), delim);
    }

    public static String join(Object[] items, String delim) {
        StringBuilder rval = new StringBuilder();
        for (int i = 0; i < items.length; i++) {
            if (i == 0) {
                rval = new StringBuilder("" + items[0]);
            } else {
                rval.append(delim).append(items[i]);
            }
        }
        return rval.toString();
    }

    /**
     * @param items
     * @param delim
     * @return
     */
    public static String join(String[] items, String delim) {
        if (null == items) {
            return null;
        }
        StringBuilder rval = new StringBuilder();
        for (int i = 0; i < items.length; i++) {
            if (i == 0) {
                rval = new StringBuilder(items[0]);
            } else {
                rval.append(delim).append(items[i]);
            }
        }
        return rval.toString();
    }

    public static String escapeXml(String in) {
        if (in == null) {
            return "";
        }
        return in.replaceAll("&", "&amp;").replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;");
    }

    public static long copy(InputStream in, OutputStream out) throws IOException {
        if (in == null) {
            throw new IOException("null InputStream");
        }
        if (out == null) {
            throw new IOException("null OutputStream");
        }
        long totalBytes = 0;
        int len = 0;
        byte[] buf = new byte[BUFFER_SIZE];
        // int available = _in.available();
        // System.err.println("DEBUG: " + _in + ": available " + available);
        while ((len = in.read(buf, 0, BUFFER_SIZE)) > -1) {
            out.write(buf, 0, len);
            out.flush();
            totalBytes += len;
            // System.err.println("DEBUG: " + _out + ": wrote " + len);
        }
        // System.err.println("DEBUG: " + _in + ": last read " + len);
        // caller MUST close the stream for us
        return totalBytes;
    }

    /**
     * @param in
     * @param out
     * @throws IOException
     */
    public static void copy(File in, File out) throws IOException {
        try (InputStream inputStream = new FileInputStream(in)) {
            OutputStream outputStream = new FileOutputStream(out);
            copy(inputStream, outputStream);
        }
    }

    public static long copy(Reader in, OutputStream out) throws IOException {
        if (in == null) {
            throw new IOException("null Reader");
        }
        if (out == null) {
            throw new IOException("null OutputStream");
        }
        OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        long len = copy(in, writer);
        // caller MUST close the stream for us
        out.flush();
        return len;
    }

    /**
     * @param in
     * @param out
     * @throws IOException
     */
    public static long copy(Reader in, Writer out) throws IOException {
        if (in == null) {
            throw new IOException("null Reader");
        }
        if (out == null) {
            throw new IOException("null Writer");
        }
        long totalChars = 0;
        int len = 0;
        char[] cbuf = new char[BUFFER_SIZE];
        while ((len = in.read(cbuf, 0, BUFFER_SIZE)) > -1) {
            out.write(cbuf, 0, len);
            out.flush();
            totalChars += len;
        }

        // caller MUST close the stream for us

        // check to see if we copied enough data
        if (1 > totalChars) {
            throw new IOException("expected at least " + 1 + " Chars, copied only " + totalChars);
        }
        return totalChars;
    }

    /**
     * @param inFilePath
     * @param outFilePath
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void copy(String inFilePath, String outFilePath) throws IOException {
        try (FileInputStream fileInputStream = new FileInputStream(inFilePath);
            FileOutputStream fileOutputStream = new FileOutputStream(outFilePath)
        ) {
            copy(fileInputStream, fileOutputStream);
        }
    }



    public static void deleteFile(File file) throws IOException {
        if (!file.exists()) {
            return;
        }
        boolean success;

        if (!file.isDirectory()) {
            success = file.delete();
            if (!success) {
                throw new IOException("error deleting " + file.getCanonicalPath());
            }
            return;
        }

        // directory, so recurse
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                // recurse
                deleteFile(child);
            }
        }

        // now this directory should be empty
        if (file.exists()) {
            file.delete();
        }
    }

    public static boolean stringToBoolean(String str) {
        // let the caller decide: should an unset string be true or false?
        return stringToBoolean(str, false);
    }

    public static boolean stringToBoolean(String str, boolean defaultValue) {
        if (str == null) {
            return defaultValue;
        }
        String lcStr = str.toLowerCase();
        return !"".equals(str) && !"0".equals(str) && !"f".equals(lcStr)
            && !"false".equals(lcStr) && !"n".equals(lcStr)
            && !"no".equals(lcStr);
    }

    /**
     * @param path
     * @throws IOException
     */
    public static void deleteFile(String path) throws IOException {
        deleteFile(new File(path));
    }

    public static String buildModulePath(Class<?> clazz) {
        return "/" + clazz.getName().replace('.', '/') + ".xqy";
    }

    public static String buildModulePath(Package pkg, String name) {
        return "/" + pkg.getName().replace('.', '/') + "/" + name
                + (name.endsWith(".xqy") ? "" : ".xqy");
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

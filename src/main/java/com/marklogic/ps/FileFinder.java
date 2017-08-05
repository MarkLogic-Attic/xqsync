/*
 * Copyright (c)2004-2017 MarkLogic Corporation
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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class FileFinder {

    private String startPath = "";

    private FileFilter filter;

    private List<File> list = new Vector<File>();

    private String includePattern;

    private String excludePattern;

    /**
     * @param _path
     */
    public FileFinder(String _path) {
        if (_path == null)
            throw new NullPointerException("starting path cannot be null");

        startPath = _path;
    }

    /**
     * @param _path
     * @param _pattern
     */
    public FileFinder(String _path, String _pattern) {
        if (_path == null)
            throw new NullPointerException("starting path cannot be null");

        startPath = _path;
        includePattern = _pattern;
    }

    /**
     * @param _f
     * @throws IOException
     */
    public FileFinder(File _f) throws IOException {
        if (_f == null) {
            throw new NullPointerException("starting path cannot be null");
        }

        startPath = _f.getCanonicalPath();
    }

    /**
     * @param _f
     * @param _pattern
     * @throws IOException
     */
    public FileFinder(File _f, String _pattern) throws IOException {
        if (_f == null) {
            throw new NullPointerException("starting path cannot be null");
        }

        startPath = _f.getCanonicalPath();
        includePattern = _pattern;
    }

    /**
     * @param _f
     * @param _includePattern
     * @param _excludePattern
     * @throws IOException
     */
    public FileFinder(File _f, String _includePattern,
            String _excludePattern) throws IOException {
        if (_f == null) {
            throw new NullPointerException("starting path cannot be null");
        }

        startPath = _f.getCanonicalPath();
        includePattern = _includePattern;
        excludePattern = _excludePattern;
    }

    /**
     * @param _path
     * @param _includePattern
     * @param _excludePattern
     */
    public FileFinder(String _path, String _includePattern,
            String _excludePattern) {
        if (_path == null)
            throw new NullPointerException("starting path cannot be null");

        startPath = _path;
        includePattern = _includePattern;
        excludePattern = _excludePattern;
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        File theFile;
        FileFinder ff;

        System.out.println("Finding all files in /tmp");
        ff = new FileFinder("/tmp");
        ff.find();
        while (ff.size() > 0) {
            theFile = ff.remove();
            System.out.println("found file: " + theFile.getCanonicalPath());
        }

        System.out.println("Finding all exe files in /tmp");
        ff = new FileFinder("/tmp", ".+\\.exe$");
        ff.find();
        while (ff.size() > 0) {
            theFile = ff.remove();
            System.out.println("found file: " + theFile.getCanonicalPath());
        }

    }

    public void find() throws IOException {
        if (filter == null) {
            if (includePattern == null && excludePattern == null) {
                // find any file
                filter = new FileFilter() {
                    public boolean accept(File _f) {
                        return _f.isDirectory() || _f.isFile();
                    }
                };
            } else if (excludePattern == null) {
                filter = new FileFilter() {
                    public boolean accept(File _f) {
                        return _f.isDirectory()
                                || (_f.isFile() && _f.getName().matches(
                                        includePattern));
                    }
                };
            } else if (includePattern == null) {
                // exclude only
                filter = new FileFilter() {
                    public boolean accept(File _f) {
                        return _f.isDirectory()
                                || (_f.isFile() && !_f.getName().matches(
                                        excludePattern));
                    }
                };
            } else {
                // both are defined
                filter = new FileFilter() {
                    public boolean accept(File _f) {
                        return _f.isDirectory()
                                || (_f.isFile()
                                        && _f.getName().matches(includePattern) && !_f
                                        .getName().matches(excludePattern));
                    }
                };
            }
        }

        if (startPath == null)
            startPath = "";

        File[] dirList = new File(startPath).listFiles(filter);

        if (dirList == null)
            return;

        for (int i = 0; i < dirList.length; i++) {
            if (dirList[i].isFile()) {
                list.add(dirList[i]);
                continue;
            }

            if (dirList[i].isDirectory()) {
                // recurse
                startPath = dirList[i].getCanonicalPath();
                find();
                continue;
            }
        }
    }

    public File remove() {
        return list.remove(0);
    }

    public int size() {
        if (list == null)
            throw new NullPointerException(
                    "FileFinder has not been initialized");

        return list.size();
    }

    public List <File>list() {
        return list;
    }

    /**
     * @return
     * @throws IOException
     */
    public List <String>listCanonicalPaths() throws IOException {
        int size = list.size();
        List <String>paths = new ArrayList<String>(size);
        Iterator<File> iter = list.iterator();
        File f;
        while (iter.hasNext()) {
            f = iter.next();
            paths.add(f.getCanonicalPath());
        }
        return paths;
    }

    /**
     * @param _root
     * @return
     */
    public List <String>listRelativePaths(String _root) {
        int rootLength = _root.length();
        int size = list.size();
        List <String>paths = new ArrayList<String>(size);
        Iterator<File> iter = list.iterator();
        File f;
        while (iter.hasNext()) {
            f = iter.next();
            paths.add(f.getAbsolutePath().substring(rootLength));
        }
        return paths;
    }

    /**
     * @param _file
     */
    public void add(File _file) {
        list.add(_file);
    }

}

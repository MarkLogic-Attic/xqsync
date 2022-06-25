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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 *
 */
public class FileFinder {

    private String startPath = "";
    private FileFilter filter;
    private final List<File> list = new ArrayList<>();
    private String includePattern;
    private String excludePattern;

    /**
     * @param path
     */
    public FileFinder(String path) {
        if (path == null) {
            throw new NullPointerException("starting path cannot be null");
        }
        startPath = path;
    }

    /**
     * @param path
     * @param pattern
     */
    public FileFinder(String path, String pattern) {
        if (path == null) {
            throw new NullPointerException("starting path cannot be null");
        }
        startPath = path;
        includePattern = pattern;
    }

    /**
     * @param file
     * @throws IOException
     */
    public FileFinder(File file) throws IOException {
        if (file == null) {
            throw new NullPointerException("starting path cannot be null");
        }

        startPath = file.getCanonicalPath();
    }

    /**
     * @param file
     * @param pattern
     * @throws IOException
     */
    public FileFinder(File file, String pattern) throws IOException {
        if (file == null) {
            throw new NullPointerException("starting path cannot be null");
        }
        startPath = file.getCanonicalPath();
        includePattern = pattern;
    }

    /**
     * @param file
     * @param includePattern
     * @param excludePattern
     * @throws IOException
     */
    public FileFinder(File file, String includePattern, String excludePattern) throws IOException {
        if (file == null) {
            throw new NullPointerException("starting path cannot be null");
        }
        startPath = file.getCanonicalPath();
        this.includePattern = includePattern;
        this.excludePattern = excludePattern;
    }

    /**
     * @param path
     * @param includePattern
     * @param excludePattern
     */
    public FileFinder(String path, String includePattern, String excludePattern) {
        if (path == null) {
            throw new NullPointerException("starting path cannot be null");
        }
        startPath = path;
        this.includePattern = includePattern;
        this.excludePattern = excludePattern;
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
                filter = file -> file.isDirectory() || file.isFile();
            } else if (excludePattern == null) {
                filter = file -> file.isDirectory() || (file.isFile() && file.getName().matches(includePattern));
            } else if (includePattern == null) {
                // exclude only
                filter = file -> file.isDirectory() || (file.isFile() && !file.getName().matches(excludePattern));
            } else {
                // both are defined
                filter = file -> file.isDirectory() || (file.isFile() && file.getName().matches(includePattern) && !file.getName().matches(excludePattern));
            }
        }

        if (startPath == null) {
            startPath = "";
        }
        File[] dirList = new File(startPath).listFiles(filter);

        if (dirList == null) {
            return;
        }
        for (File file : dirList) {
            if (file.isFile()) {
                list.add(file);
                continue;
            }

            if (file.isDirectory()) {
                // recurse
                startPath = file.getCanonicalPath();
                find();
                continue;
            }
        }
    }

    public File remove() {
        return list.remove(0);
    }

    public int size() {
        if (list == null) {
            throw new NullPointerException("FileFinder has not been initialized");
        }
        return list.size();
    }

    public List<File>list() {
        return list;
    }

    /**
     * @return
     * @throws IOException
     */
    public List<String>listCanonicalPaths() throws IOException {
        int size = list.size();
        List <String>paths = new ArrayList<>(size);
        Iterator<File> iter = list.iterator();
        File f;
        while (iter.hasNext()) {
            f = iter.next();
            paths.add(f.getCanonicalPath());
        }
        return paths;
    }

    /**
     * @param root
     * @return
     */
    public List<String>listRelativePaths(String root) {
        int rootLength = root.length();
        int size = list.size();
        List <String>paths = new ArrayList<>(size);
        Iterator<File> iter = list.iterator();
        File f;
        while (iter.hasNext()) {
            f = iter.next();
            paths.add(f.getAbsolutePath().substring(rootLength));
        }
        return paths;
    }

    /**
     * @param file
     */
    public void add(File file) {
        list.add(file);
    }

}

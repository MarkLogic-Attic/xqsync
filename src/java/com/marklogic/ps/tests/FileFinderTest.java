/**
 * Copyright (c) 2006 Mark Logic Corporation. All rights reserved.
 */
package com.marklogic.ps.tests;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import com.marklogic.ps.FileFinder;
import com.marklogic.ps.Utilities;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class FileFinderTest extends TestCase {

    /*
     * Test method for 'com.marklogic.ps.FileFinder.listRelativePaths(String)'
     */
    public void testListRelativePaths() throws IOException {
        String basePath = "/test/foo/bar";
        FileFinder ff = new FileFinder(basePath);
        // fake the list contents
        ff.add(new File(basePath + "/test"));
        ff.add(new File(basePath + "/baz/test"));
        ff.add(new File(basePath + "/baz/buz/test1"));
        List list = ff.listRelativePaths("/");
        String testString = Utilities.join(list, ":");
        String expected = "test/foo/bar/test" + ":test/foo/bar/baz/test"
                + ":test/foo/bar/baz/buz/test1";
        assertEquals(testString, expected);
    }

}

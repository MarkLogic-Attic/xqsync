/**
 * Copyright (c) 2006-2022 MarkLogic Corporation. All rights reserved.
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
import java.io.IOException;
import java.util.List;

import com.marklogic.ps.FileFinder;
import com.marklogic.ps.Utilities;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Michael Blakeley, MarkLogic Corporation
 * 
 */
public class FileFinderTest {

    /*
     * Test method for 'com.marklogic.ps.FileFinder.listRelativePaths(String)'
     */
    @Test
    public void testListRelativePaths() {
        String basePath = "/test/foo/bar";
        FileFinder ff = new FileFinder(basePath);
        // fake the list contents
        ff.add(new File(basePath + "/test"));
        ff.add(new File(basePath + "/baz/test"));
        ff.add(new File(basePath + "/baz/buz/test1"));
        List<String> list = ff.listRelativePaths("/");
        String testString = Utilities.join(list, ":");
        String expected = "test/foo/bar/test" + ":test/foo/bar/baz/test" + ":test/foo/bar/baz/buz/test1";
        assertEquals(testString, expected);
    }

  @Test
  public void findJson() {
    File theFile;
    FileFinder fileFinder = new FileFinder("src/main/ml-config", ".*json$", ".*xqy");
    try {
        fileFinder.find();
        assertNotEquals(0, fileFinder.size());
        while (fileFinder.size() > 0) {
            theFile = fileFinder.remove();
            assertTrue(theFile.getCanonicalPath().endsWith(".json"));
        }
        assertEquals(0, fileFinder.size());
    } catch (IOException ex) {
        fail();
    }
  }

  @Test
  public void findXQuery() {
    FileFinder fileFinder = new FileFinder("src/main/ml-modules", ".*xqy");
    try {
      fileFinder.find();
      assertEquals(1, fileFinder.size());
    } catch (IOException ex) {
      fail();
    }
  }

}

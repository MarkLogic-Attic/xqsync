package com.marklogic.ps;

import com.marklogic.ps.xqsync.XQSyncManager;
import junit.framework.TestCase;

public class UtilitiesTest extends TestCase {

  public void testBuidModulePath() {
    assertEquals("/com/marklogic/ps/xqsync/XQSyncManager.xqy", Utilities.buildModulePath(XQSyncManager.class));
  }

  public void testBuidModulePathWithPackage() {
    String result = "/com/marklogic/ps/xqsync/XQSyncManager.xqy";
    assertEquals(result, Utilities.buildModulePath(XQSyncManager.class.getPackage(), "XQSyncManager"));
    assertEquals(result, Utilities.buildModulePath(XQSyncManager.class.getPackage(), "XQSyncManager.xqy"));
  }

}
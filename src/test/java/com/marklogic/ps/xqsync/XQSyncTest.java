package com.marklogic.ps.xqsync;

import com.marklogic.ps.SimpleLogger;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Created by mhansen on 7/9/17.
 */
public class XQSyncTest {
/*
    @Test
    public void main() throws Exception {
        String[] args = new String[]{};
        XQSync.main(args);
    }
*/
    @Test (expected = NullPointerException.class)
    public void mainWithNullArgs() throws Exception {
        XQSync.main(null);
    }

    @Test
    public void initConfiguration() throws Exception {
        String expected = Configuration.CONFIGURATION_CLASSNAME_DEFAULT;
        Properties properties = new Properties();
        properties.setProperty(Configuration.CONFIGURATION_CLASSNAME_KEY, expected);
        properties.setProperty(Configuration.INPUT_CONNECTION_STRING_KEY, "xcc://admin:admin@localhost:9000");
        properties.setProperty(Configuration.OUTPUT_CONNECTION_STRING_KEY, "xcc://admin:admin@localhost:9000");
        Configuration configuration = XQSync.initConfiguration(SimpleLogger.getSimpleLogger(), properties);
        assertNotNull(configuration);
        assertEquals(expected, configuration.getConfigurationClassName());
    }

    @Test (expected = FatalException.class)
    public void initConfigurationWithNullLoggerAndConfigurationClassname() throws Exception {
        String expected = "foo";
        Properties properties = new Properties();
        properties.setProperty(Configuration.CONFIGURATION_CLASSNAME_KEY, expected);
        Configuration configuration = XQSync.initConfiguration(null, properties);
        //should this throw NPE or just not attempt to log if the logger is null?
    }

    @Test (expected = FatalException.class)
    public void initConfigurationWithoutConfigurationClassname() throws Exception {
        Properties properties = new Properties();
        XQSync.initConfiguration(null, properties);
    }

    @Test (expected = FatalException.class)
    public void initConfigurationWithNullConfigAndProperties() throws Exception {
        XQSync.initConfiguration(null, null);
    }

    @Test
    public void getClassLoader() throws Exception {
        assertNotNull(XQSync.getClassLoader());
    }

}
/**
 * Copyright (c) 2008-2012 Mark Logic Corporation. All rights reserved.
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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import com.marklogic.ps.SimpleLogger;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 *
 */
public class AbstractConfiguration {

    protected static final String DEFAULT_SUFFIX = "_DEFAULT";

    protected static final String KEY_SUFFIX = "_KEY";

    protected static SimpleLogger logger;

    protected Map<String, Object> defaults = new HashMap<String, Object>();

    protected Properties properties = new Properties();

    protected void setDefaults() throws IllegalArgumentException,
            SecurityException, IllegalAccessException,
            NoSuchFieldException {
        Field[] fields = this.getClass().getFields();
        String name, key;
        for (int i = 0; i < fields.length; i++) {
            name = fields[i].getName();
            if (name.endsWith(KEY_SUFFIX)) {
                key = (String) fields[i].get(this);
                if (!defaults.containsKey(key)) {
                    defaults.put(key, null);
                    logger.fine(key + "=(null)");
                }
                continue;
            }
            if (name.endsWith(DEFAULT_SUFFIX)) {
                // the true key is the value of the key-named field
                key = (String) this.getClass().getField(
                        name.substring(0, name.length()
                                - DEFAULT_SUFFIX.length())
                                + KEY_SUFFIX).get(this);
                defaults.put(key, fields[i].get(this));
                logger.fine(key + "=" + defaults.get(key));
                continue;
            }
        }
    }

    /**
     * @param _props
     */
    public void load(Properties _props) {
        properties.putAll(_props);
    }

    /**
     * @param _stream
     * @throws IOException
     */
    public void load(InputStream _stream) throws IOException {
        Properties newProperties = new Properties();
        newProperties.load(_stream);
        load(newProperties);
    }

    /**
     * validate user-defined property names and apply defaults
     */
    protected void validateProperties() {
        Properties validated = new Properties();
        Enumeration<?> keys = properties.propertyNames();
        // ignore known patterns from System properties
        String ignorePrefixPatterns = "^("
            + "awt|file|ftp|http|https|java|line|mrj|os|path|sun|user"
            + ")\\..+";
        String ignorePatterns = "^(gopherProxySet|socksNonProxyHosts)$";
        String key, value;
        while (keys.hasMoreElements()) {
            key = (String) keys.nextElement();
            // known jre pattern
            if (key.matches(ignorePatterns)
                || key.matches(ignorePrefixPatterns)) {
                logger.fine("known system key: ignoring " + key);
                continue;
            }
            // key is unknown
            if (!defaults.containsKey(key)) {
                logger.warning("unknown key: skipping " + key);
                continue;
            }
            // known key
            value = properties.getProperty(key);
            logger.info("using " + key + "=" + value);
            validated.setProperty(key, value);
        }

        applyDefaults(validated);

        // use the validated properties
        properties = validated;
    }

    /**
     * @param _props
     */
    private void applyDefaults(Properties _props) {
        // apply default values to properties
        Iterator<String> iter = defaults.keySet().iterator();
        String key;
        Object value;
        while (iter.hasNext()) {
            key = iter.next();
            if (_props.containsKey(key)) {
                continue;
            }
            value = defaults.get(key);
            if (null != value) {
                logger.fine("applying default " + key + "=" + value);
                _props.setProperty(key.toString(), value.toString());
            }
        }
    }

    /**
     * @return
     */
    public SimpleLogger getLogger() {
        return logger;
    }

    /**
     * @return
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * @param _logger
     */
    public void setLogger(SimpleLogger _logger) {
        logger = _logger;
    }

}

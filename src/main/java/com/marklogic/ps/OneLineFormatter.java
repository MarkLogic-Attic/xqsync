/** -*- mode: java; indent-tabs-mode: nil; c-basic-offset: 4; -*-
 *
 * Copyright (c)2005-2022 MarkLogic Corporation
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

import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.text.SimpleDateFormat;
import java.text.FieldPosition;
import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * An implementation of Formatter that tries to fit everything on one line
 */
public class OneLineFormatter extends Formatter {

    private final Date dat = new Date();
    private final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private final FieldPosition pos = new FieldPosition(0);

    // Line separator string.  This is the value of the line.separator
    // property at the moment that the OneLineFormatter was created.
    private final String lineSeparator = System.getProperty("line.separator");

    /**
     * Format the given LogRecord.
     * <p>
     * This method can be overridden in a subclass.
     * It is recommended to use the {@link Formatter#formatMessage}
     * convenience method to localize and format the message field.
     *
     * @param logRecord the log record to be formatted.
     * @return a formatted log record
     */
    public synchronized String format(LogRecord logRecord) {

        StringBuffer sb = new StringBuffer();

        // Minimize memory allocations here.
        dat.setTime(logRecord.getMillis());
        formatter.format(dat, sb, pos);

        sb.append(" ");

        String message = formatMessage(logRecord);
        sb.append(logRecord.getLevel().getLocalizedName());
        sb.append(": ");
        sb.append(message);
        sb.append(lineSeparator);
        if (logRecord.getThrown() != null) {
            try {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                logRecord.getThrown().printStackTrace(pw);
                pw.close();
                sb.append(sw);
            } catch (Exception ex) {
            }
        }
        return sb.toString();
    }
}

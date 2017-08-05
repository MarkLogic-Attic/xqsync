/*
 * Copyright (c)2005-2017 MarkLogic Corporation
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

package com.marklogic.ps.timing;

/*
 * @author Michael Blakeley, MarkLogic Corporation
 */
public class TimedEvent {
    private long bytes = 0;

    private long duration = -1;

    private boolean error = false;

    private long start;
    
    private String description = null;

    public TimedEvent() {
        start = System.nanoTime();
    }

    public TimedEvent(boolean _error) {
        error = _error;
        start = System.nanoTime();
    }

    /**
     * @return
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * 
     */
    public long stop() {
        return stop(bytes < 0 ? 0 : bytes, false);
    }

    public long stop(long _bytes) {
        return stop(_bytes, false);
    }

    /**
     * @param _error
     */
    public void stop(boolean _error) {
        stop(-1, _error);
    }

    public long stop(long _bytes, boolean _error) {
        // duplicate calls to stop() should be harmless
        if (duration > -1) {
            return duration;
        }

        duration = System.nanoTime() - start;
        // timings of 0 are very bad for averaging,
        // but is the cure worse than the disease?
        assert duration > 0;
        // alternative idea:
        //if (duration < 1) {
        //duration = 1;
        //}

        // bytes == -1 is a flag
        if (_bytes > -1) {
            bytes = _bytes;
        }
        error = _error;
        return duration;
    }

    /**
     * @return
     */
    public long getDuration() {
        if (duration < 0) {
            return System.nanoTime() - start;
        }
        
        return duration;
    }

    /**
     * @return
     */
    public boolean isError() {
        return error;
    }

    /**
     * @return
     */
    public long getStart() {
        return start;
    }

    public void increment(long _bytes) {
        bytes += _bytes;
    }

    /**
     * @param b
     */
    public void setError(boolean b) {
        error = true;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}

/**
 * Copyright (c) 2007-2017 MarkLogic Corporation. All rights reserved.
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

import org.junit.Test;

import java.sql.Time;

import static org.junit.Assert.*;

/**
 *
 */
public class TimerTest {
    @Test
    public void addZeroEvents() throws Exception {
        Timer timer = new Timer();
        timer.add(timer);
        assertEquals(0,timer.getEventCount());
        assertEquals(0, timer.getErrorCount());
    }

    @Test
    public void addTimedEvent() throws Exception {
        Timer timer = new Timer();
        TimedEvent timedEvent = new TimedEvent();
        timedEvent.setDescription("my timer event");
        timer.add(timedEvent);
        assertEquals(1,timer.getEventCount());
        assertEquals(0, timer.getErrorCount());
    }

    @Test
    public void addTimedEventButNotKeep() throws Exception {
        Timer timer = new Timer();
        TimedEvent timedEvent = new TimedEvent();
        timedEvent.setDescription("my timer event");
        timer.add(timedEvent, false);
        assertEquals(1,timer.getEventCount());
        assertEquals(0, timer.getErrorCount());
    }

    @Test
    public void addErrorTimedEvent() throws Exception {
        TimedEvent timedEvent = new TimedEvent(true);
        timedEvent.setError(true);

        Timer timer = new Timer();
        timer.add(timedEvent);
        assertEquals(1,timer.getEventCount());
        assertEquals(0, timer.getErrorCount());
    }

    @Test
    public void getBytesPerSecondWithZeroBytes() {
        Timer timer = new Timer();
        assertEquals(0d,timer.getBytesPerSecond(), 0.00001);
    }

    @Test
    public void getBytesPerSecond() {
        TimedEvent timedEvent = new TimedEvent();
        timedEvent.increment(50000l);
        Timer timer = new Timer();
        timer.add(timedEvent);
        assertNotEquals(0d,timer.getBytesPerSecond(), 0.00001);
    }

    @Test
    public void getSuccessfuleEventCountWithoutEvents() {
        Timer timer = new Timer();
        assertEquals(0, timer.getSuccessfulEventCount());
    }

    @Test
    public void getSuccessfuleEventCount() {
        TimedEvent timedEvent = new TimedEvent();
        Timer timer = new Timer();
        timer.add(timedEvent);
        assertEquals(1, timer.getSuccessfulEventCount());
    }

    @Test
    public void getCurrProgressMessageIsNullWhenLittleTimePassed() {
        Timer timer = new Timer();
        assertNull(timer.getCurrProgressMessage());
    }

    @Test
    public void getProgressMessage() {
        Timer timer = new Timer();
        assertEquals("0 events/s, 0 kB/s", timer.getProgressMessage());
    }
}
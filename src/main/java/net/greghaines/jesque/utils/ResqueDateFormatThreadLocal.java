/*
 * Copyright 2011 Greg Haines
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.greghaines.jesque.utils;

import static net.greghaines.jesque.utils.ResqueConstants.DATE_FORMAT;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Provides DateFormats configured for use with Resque.
 * 
 * @author Greg Haines
 */
public final class ResqueDateFormatThreadLocal extends ThreadLocal<DateFormat> {
    
    private static volatile ResqueDateFormatThreadLocal instance = null;
    private static final Object instanceLock = new Object();

    /**
     * NOTE: DateFormats returned from this method are for use by the caller's
     * thread only.
     * 
     * @return a configured DateFormat
     */
    public static DateFormat getInstance() {
        if (instance == null) {
            synchronized (instanceLock) {
                if (instance == null) {
                    instance = new ResqueDateFormatThreadLocal();
                }
            }
        }
        return instance.get();
    }

    private ResqueDateFormatThreadLocal() {
        // Singleton
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DateFormat initialValue() {
        final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return sdf;
    }
}

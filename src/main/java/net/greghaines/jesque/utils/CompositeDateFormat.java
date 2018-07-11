/*
 * Copyright 2013 Greg Haines
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

import java.io.Serializable;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;

/**
 * CompositeDateFormat attempts to parse dates using several known date format patterns 
 * and formats dates using {@link ResqueDateFormatThreadLocal#getInstance()}.
 * 
 * @author Greg Haines
 */
public class CompositeDateFormat extends DateFormat {

    private static final long serialVersionUID = -4079876635509458541L;
    private static final List<DateFormatFactory> DATE_FORMAT_FACTORIES = Arrays.asList(
        new DateFormatFactory() {

            /**
             * {@inheritDoc}
             */
            @Override
            public DateFormat create() {
                return ResqueDateFormatThreadLocal.getInstance();
            }
        },
        new PatternDateFormatFactory(ResqueConstants.DATE_FORMAT_RUBY_V1),
        new PatternDateFormatFactory(ResqueConstants.DATE_FORMAT_RUBY_V2),
        new PatternDateFormatFactory(ResqueConstants.DATE_FORMAT_RUBY_V3),
        new PatternDateFormatFactory(ResqueConstants.DATE_FORMAT_RUBY_V4),
        new PatternDateFormatFactory(ResqueConstants.DATE_FORMAT_PHP)
    );

    public CompositeDateFormat() {
        super();
        setCalendar(new GregorianCalendar());
        setNumberFormat(new DecimalFormat());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StringBuffer format(final Date date, final StringBuffer toAppendTo, final FieldPosition fieldPosition) {
        return ResqueDateFormatThreadLocal.getInstance().format(date, toAppendTo, fieldPosition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date parse(final String dateStr, final ParsePosition pos) {
        final ParsePosition posCopy = new ParsePosition(pos.getIndex());
        Date date = null;
        boolean success = false;
        for (final DateFormatFactory dfFactory : DATE_FORMAT_FACTORIES) {
            posCopy.setIndex(pos.getIndex());
            posCopy.setErrorIndex(pos.getErrorIndex());
            date = dfFactory.create().parse(dateStr, posCopy);
            if (date != null) {
                success = true;
                break;
            }
        }
        if (success) {
            pos.setIndex(posCopy.getIndex());
            pos.setErrorIndex(posCopy.getErrorIndex());
        }
        return date;
    }

    private interface DateFormatFactory {
        DateFormat create();
    }

    private static class PatternDateFormatFactory implements DateFormatFactory, Serializable {

        private static final long serialVersionUID = 3382491374377384377L;

        private final String pattern;

        /**
         * Constructor.
         * 
         * @param pattern
         *            the date format pattern
         */
        public PatternDateFormatFactory(final String pattern) {
            this.pattern = pattern;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DateFormat create() {
            final SimpleDateFormat dateFormat = new SimpleDateFormat(this.pattern, Locale.US);
            dateFormat.setLenient(false);
            return dateFormat;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "PatternDateFormatFactory [pattern=" + this.pattern + "]";
        }
    }
}

package net.greghaines.jesque.json;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

import net.greghaines.jesque.utils.CompositeDateFormat;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCompositeDateFormat {

    private static Date date;

    @BeforeClass
    public static void beforeClass() {
        final Calendar cal = Calendar.getInstance(Locale.US);
        cal.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        cal.set(Calendar.YEAR, 2013);
        cal.set(Calendar.MONTH, Calendar.MARCH);
        cal.set(Calendar.DATE, 7);
        cal.set(Calendar.HOUR_OF_DAY, 21);
        cal.set(Calendar.MINUTE, 26);
        cal.set(Calendar.SECOND, 05);
        cal.set(Calendar.MILLISECOND, 234);
        date = cal.getTime();
    }

    @Test
    public void testParse_ISO8601() throws ParseException {
        final DateFormat dateFormat = new CompositeDateFormat();
        // yyyy-MM-dd'T'HH:mm:ss.SSSZ
        Assert.assertEquals(date, dateFormat.parse("2013-03-07T21:26:05.234-0500"));
        Assert.assertEquals(date, dateFormat.parse("2013-03-08T02:26:05.234+0000"));
    }

    @Test
    public void testParse_PHP() throws ParseException {
        final DateFormat dateFormat = new CompositeDateFormat();
        // EEE MMM dd HH:mm:ss z yyyy
        // Since PHP's format is missing milliseconds, see if they're within a
        // second of each other
        assertWithinASecond(date, dateFormat.parse("Thu March 07 21:26:05 GMT-05:00 2013"));
        assertWithinASecond(date, dateFormat.parse("Fri March 08 02:26:05 GMT 2013"));
    }

    @Test
    public void testParse_Ruby() throws ParseException {
        final DateFormat dateFormat = new CompositeDateFormat();
        // yyyy-MM-dd HH:mm:ss Z
        // Since Ruby's formats are missing milliseconds, see if they're within
        // a second of each other
        assertWithinASecond(date, dateFormat.parse("2013-03-07 21:26:05 -0500"));
        assertWithinASecond(date, dateFormat.parse("2013-03-08 02:26:05 +0000"));
        assertWithinASecond(date, dateFormat.parse("2013/03/07 21:26:05 -0500"));
        assertWithinASecond(date, dateFormat.parse("2013/03/08 02:26:05 +0000"));
    }

    @Test
    public void testFormat() {
        final DateFormat dateFormat = new CompositeDateFormat();
        Assert.assertEquals("2013-03-08T02:26:05.234+0000", dateFormat.format(date));
    }

    private static void assertWithinASecond(final Date expected, final Date actual) {
        final double delta = expected.getTime() - actual.getTime();
        final String msg = "expected=" + expected + " actual=" + actual + " delta=" + delta;
        Assert.assertTrue(msg, Math.abs(delta) < 1000);
    }
}

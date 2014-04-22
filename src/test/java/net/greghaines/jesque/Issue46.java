package net.greghaines.jesque;

import static net.greghaines.jesque.TestUtils.createJedis;
import static net.greghaines.jesque.utils.JesqueUtils.createKey;
import static net.greghaines.jesque.utils.JesqueUtils.entry;
import static net.greghaines.jesque.utils.JesqueUtils.map;
import static net.greghaines.jesque.utils.ResqueConstants.FAILED;
import static net.greghaines.jesque.utils.ResqueConstants.PROCESSED;
import static net.greghaines.jesque.utils.ResqueConstants.STAT;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import net.greghaines.jesque.worker.MapBasedJobFactory;
import net.greghaines.jesque.worker.Worker;
import net.greghaines.jesque.worker.WorkerImpl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

/**
 * Tests problem described in Issue #46.
 */
public class Issue46 {
    
    private static final Logger LOG = LoggerFactory.getLogger(Issue46.class);
    private static final Config CONFIG = new ConfigBuilder().build();
    private static final String TEST_QUEUE = "foo";

    @Before
    public void resetRedis() {
        TestUtils.resetRedis(CONFIG);
    }
    
    @Test
    public void testIssue46() {
        final Map<String, Object> templateValues = new HashMap<String, Object>();
        templateValues.put("email", "mail@gmail.com");
        templateValues.put("url", "http://localhost:90/confir");
        templateValues.put("fName", "RANGANATHAN");
        final Job job = new Job("SendMailJob", null, "mail@gmail.com", "REGISTRATION_MAIL_SUBJECT", true, 
                "REGISTRATION_MAIL", templateValues);
        final Worker worker = new WorkerImpl(CONFIG, Arrays.asList(TEST_QUEUE), 
                new MapBasedJobFactory(map(entry("SendMailJob", SendMailJob.class))));
        final Thread workerThread = new Thread(worker);
        workerThread.start();
        try {
            TestUtils.enqueueJobs(TEST_QUEUE, Arrays.asList(job), CONFIG);
        } finally {
            TestUtils.stopWorker(worker, workerThread);
        }
        final Jedis jedis = createJedis(CONFIG);
        try {
            Assert.assertEquals("1", jedis.get(createKey(CONFIG.getNamespace(), STAT, PROCESSED)));
            Assert.assertNull(jedis.get(createKey(CONFIG.getNamespace(), STAT, FAILED)));
        } finally {
            jedis.quit();
        }
    }

    public static class SendMailJob implements Runnable {
        
        private final String mailFrom;
        private final String mailTo;
        private final String subject;
        private final Boolean isHtml;
        private final String templateName;
        private final Map<String, Object> templateValues;
        
        public SendMailJob(final String mailFrom, final String mailTo, final String subject, final Boolean isHtml, 
                final String templateName, final Map<String, Object> templateValues) {
            this.mailFrom = mailFrom;
            this.mailTo = mailTo;
            this.subject = subject;
            this.isHtml = isHtml;
            this.templateName = templateName;
            this.templateValues = templateValues;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run() {
            LOG.info("mailFrom={} mailTo={} subject={}, isHtml={}, templateName={}, templateValues={}", new Object[]{
                    this.mailFrom, this.mailTo, this.subject, this.isHtml, this.templateName, this.templateValues});
        }
    }
}

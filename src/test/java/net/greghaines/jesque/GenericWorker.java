package net.greghaines.jesque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericWorker implements Runnable {
		private static final Logger log = LoggerFactory.getLogger(TestAction.class);
		
		private final Integer i;
		private final Double d;
		private final String s;
		
		
		public GenericWorker(final Object... args) {
			this.i = (Integer) args[0];
			this.d = (Double) args[1];
			this.s = (String) args[3];
		}

		public GenericWorker(final Integer i, final Double d, final String s)
		{
			this.i = i;
			this.d = d;
			this.s = s;
		}

		public void run()
		{
			log.info("GenericWorker.run() {} {} {}", new Object[]{this.i, this.d, this.s});
			try { Thread.sleep(100); } catch (Exception e){}
		}

}

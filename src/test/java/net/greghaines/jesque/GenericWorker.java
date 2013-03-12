package net.greghaines.jesque;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericWorker implements Runnable {
		private static final Logger log = LoggerFactory.getLogger(TestAction.class);
		
		private final Integer i;
		private final Double d;
		private final String s;
		private final List<Object> l;

		
		
		public GenericWorker(final Object... args) {
			this.i = (Integer) args[0];
			this.d = (Double) args[1];
			this.s = (String) args[2];
			this.l = (List<Object>) args[3];
		}

		public GenericWorker(final Integer i, final Double d, final String s)
		{
			this.i = i;
			this.d = d;
			this.s = s;
			this.l = null;
		}

		public void run()
		{
			log.info("GenericWorker.run() {} {} {}", new Object[]{this.i, this.d, this.s});
			log.info("GenericWorker.run() got a List object: " + ((l != null) ? "true" :"false") );
			try { Thread.sleep(100); } catch (Exception e){}
		}

}

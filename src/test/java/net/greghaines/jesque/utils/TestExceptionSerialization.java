package net.greghaines.jesque.utils;

import static net.greghaines.jesque.utils.JesqueUtils.createBacktrace;
import static net.greghaines.jesque.utils.JesqueUtils.recreateException;

import org.junit.Assert;
import org.junit.Test;

public class TestExceptionSerialization
{
	@Test
	public void simpleNoMsg()
	throws Exception
	{
		try
		{
			throw new Exception();
		}
		catch (Throwable ex)
		{
			serialize(ex);
		}
	}
	
	@Test
	public void simpleWithMsg()
	throws Exception
	{
		try
		{
			throw new Exception("FOO: tricky!");
		}
		catch (Throwable ex)
		{
			serialize(ex);
		}
	}
	
	@Test
	public void nestedOneDeepNoMsg()
	throws Exception
	{
		try
		{
			try
			{
				throw new Exception();
			}
			catch (Throwable t1)
			{
				throw new RuntimeException(t1);
			}
		}
		catch (Throwable ex)
		{
			serialize(ex);
		}
	}
	
	@Test
	public void nestedOneDeepWithDeepMsg()
	throws Exception
	{
		try
		{
			try
			{
				throw new Exception("FOO: tricky!");
			}
			catch (Throwable t1)
			{
				throw new RuntimeException(t1);
			}
		}
		catch (Throwable ex)
		{
			serialize(ex);
		}
	}
	
	@Test
	public void nestedOneDeepWithSurfaceMsg()
	throws Exception
	{
		try
		{
			try
			{
				throw new Exception();
			}
			catch (Throwable t1)
			{
				throw new RuntimeException("FOO: tricky!", t1);
			}
		}
		catch (Throwable ex)
		{
			serialize(ex);
		}
	}
	
	@Test
	public void nestedOneDeepWithBothMsg()
	throws Exception
	{
		try
		{
			try
			{
				throw new Exception("Thar' she blows...");
			}
			catch (Throwable t1)
			{
				throw new RuntimeException("FOO: tricky!", t1);
			}
		}
		catch (Throwable ex)
		{
			serialize(ex);
		}
	}
	
	@Test
	public void nestedSuperDeepWithCrazyMsg()
	throws Exception
	{
		try
		{
			try
			{
				try
				{
					try
					{
						throw new Exception("Thar' she blows...");
					}
					catch (Throwable t3)
					{
						throw new IllegalStateException(t3);
					}
				}
				catch (Throwable t2)
				{
					throw new IllegalArgumentException(t2);
				}
			}
			catch (Throwable t1)
			{
				throw new RuntimeException("FOO: tricky!", t1);
			}
		}
		catch (Throwable ex)
		{
			serialize(ex);
		}
	}
	
	private static void serialize(final Throwable ex)
	throws Exception
	{
		assertEquals(ex, recreateException(ex.getClass().getName(), ex.getMessage(), createBacktrace(ex)));
	}

	private static void assertEquals(final Throwable ex, final Throwable newEx)
	{
		Assert.assertEquals((ex == null), (newEx == null));
		if (ex != null)
		{
			Assert.assertEquals(ex.getClass(), newEx.getClass());
			Assert.assertEquals(ex.getMessage(), newEx.getMessage());
			Assert.assertEquals((ex.getCause() == null), (newEx.getCause() == null));
			Assert.assertEquals(createBacktrace(ex), createBacktrace(newEx));
			if (ex.getCause() != null)
			{
				assertEquals(ex.getCause(), newEx.getCause());
			}
		}
	}
}

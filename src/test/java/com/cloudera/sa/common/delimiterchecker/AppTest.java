package com.cloudera.sa.common.delimiterchecker;

import junit.framework.Assert;

import org.junit.Test;


/**
 * Unit test for simple App.
 */
public class AppTest 
{
	@Test
	public void testDelimiterCounter() {
		DelimiterCheckerJob.CustomMapper.setDelimiter('|');
		
		Assert.assertEquals(5, DelimiterCheckerJob.CustomMapper.countColumns("a|a|a|a|a"));
		Assert.assertEquals(5, DelimiterCheckerJob.CustomMapper.countColumns("a|a|a|a|"));
		Assert.assertEquals(5, DelimiterCheckerJob.CustomMapper.countColumns("|a|a|a|a"));
		Assert.assertEquals(5, DelimiterCheckerJob.CustomMapper.countColumns("||||"));
		Assert.assertEquals(5, DelimiterCheckerJob.CustomMapper.countColumns("|||asdf|"));
	}
	
}

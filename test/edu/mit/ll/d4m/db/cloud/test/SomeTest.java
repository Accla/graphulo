package edu.mit.ll.d4m.db.cloud.test;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Test;

public class SomeTest {

	@Test
	public void test() {
		long val = new BigDecimal("7.0000e+09").longValue();
		System.out.println("val is "+val);
	}

}

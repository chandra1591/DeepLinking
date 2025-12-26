package com.chandra.test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;

public class FibonacciUseCaseAsyncTest {
    FibonacciUseCaseAsync.Callback callback;
    FibonacciUseCaseAsync SUT;

    BigInteger lastResult  = null;

    @Before
    public void setup() throws Exception{
        callback = result -> lastResult = result;
        SUT = new FibonacciUseCaseAsync();
    }

    @Test
    public void computeFibonacci_return0() throws Exception{
        SUT.computedFibonacci(0, callback);
         Thread.sleep(10);
        assertThat(lastResult, is(new BigInteger("0")));
    }

    @Test
    public void computeFibonacci_return1() throws Exception{
        SUT.computedFibonacci(1, callback);
        Thread.sleep(10);
        assertThat(lastResult, is(new BigInteger("1")));
    }

    @Test
    public void computeFibonacci_return() throws Exception{
        SUT.computedFibonacci(30, callback);
        Thread.sleep(200);
        assertThat(lastResult, is(new BigInteger("832040")));
    }

}

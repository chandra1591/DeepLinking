package com.chandra.test;

import android.os.Handler;
import android.os.Looper;
import android.telecom.Call;

import androidx.annotation.WorkerThread;

import java.math.BigInteger;

public class FibonacciUseCaseAsync {

    public interface Callback{
        void onFibonacciComputed(BigInteger result);
    }

    public void computedFibonacci(int index, Callback callback){
        new Thread(() -> {
            BigInteger result  = computedFibonacciBig(index);
            new Handler(Looper.getMainLooper()).post(new Runnable() {
                @Override
                public void run() {


                }
            });
            callback.onFibonacciComputed(result);
        }).start();
    }

    @WorkerThread
    private BigInteger computedFibonacciBig(int index) {
        if(index ==0){
            return new BigInteger("0");
        }else if(index == 1){
            return new BigInteger("1");
        }else{
            return computedFibonacciBig(index - 1).add(computedFibonacciBig(index - 2));
        }
    }
}

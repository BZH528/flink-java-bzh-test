package com.bzh.bigdata.javase;

import java.util.Date;

public class MyRunable implements Runnable {

    private String command;

    public MyRunable(String command) {
        this.command = command;
    }

    @Override
    public void run() {
        System.out.println(Thread.currentThread().getName() + "\tStart time: " + new Date());
        processCommand();
        System.out.println(Thread.currentThread().getName() + "\tEnd time: " + new Date());
    }

    private void processCommand() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "MyRunable{" +
                "command='" + command + '\'' +
                '}';
    }
}

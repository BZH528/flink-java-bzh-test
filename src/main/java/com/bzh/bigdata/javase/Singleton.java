package com.bzh.bigdata.javase;

public class Singleton {

    private volatile  static Singleton singletonInstance;

    private Singleton() {

    }

    public  static Singleton getSingletonInstace() {

        if (singletonInstance == null) {

            synchronized (Singleton.class) {
                if (singletonInstance == null) {
                    singletonInstance = new Singleton();
                }
            }

        }

        return singletonInstance;
    }


    public static void main(String[] args) {

        for (int i = 1; i <= 10; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName() + ": " + Singleton.getSingletonInstace().hashCode());
                }
            }).start();
        }
    }
}

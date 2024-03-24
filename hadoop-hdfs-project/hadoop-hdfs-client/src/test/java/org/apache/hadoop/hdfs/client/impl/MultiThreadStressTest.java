package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiThreadStressTest {
    final static URI uri;

    static {
        try {
            uri = new URI("hdfs://59.111.211.47:9000");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    final static Configuration configuration = new Configuration();

    final static String user = "root";
    final static FileSystem fs;
    static Integer id = 1219560000;

    static {
        try {
            fs = FileSystem.get(uri, configuration, user);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public MultiThreadStressTest() throws URISyntaxException {
    }

    public static void main(String[] args) throws Exception {


        int threads = 1000; // 设置线程数
        int requestsPerThread = 10000; // 每个线程发起的请求数

        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executorService.submit(() -> {
                try {
                    startLatch.await(); // 等待所有线程准备好
                    for (int j = 0; j < requestsPerThread; j++) {
                        extracted(j);
                        System.out.println("111111111111");
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } finally {
                    endLatch.countDown(); // 完成一个线程的任务
                }
            });
        }

        long startTime = System.currentTimeMillis();
        startLatch.countDown(); // 开始压测
        endLatch.await(); // 等待所有线程完成
        long endTime = System.currentTimeMillis();

        System.out.println("压测完成，耗时: " + (endTime - startTime) + "毫秒");
        executorService.shutdown();
    }

    private static synchronized void extracted(int j) throws IOException {
        // 在此处添加需要压测的方法调用，例如：myTestMethod();
        id = id + j;
        String file = String.valueOf(id);
        System.out.println(file);
        FSDataOutputStream fos = fs.create(new Path("/test/file4/" + file + ".txt"));
        fos.write("hello world ".getBytes());
        fos.close();
    }
}
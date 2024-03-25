package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 进行hdfs并发性能测试
 */
public class HdfsPressThread implements Runnable {
    //链接集群NameNode地址
    Path path = new Path("/");
    URI uri = new URI("hdfs://127.0.0.1:9000");
    private CountDownLatch countDownLatch;

    public HdfsPressThread(int i, CountDownLatch countDownLatch) throws Exception {
        this.countDownLatch = countDownLatch;
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        //创建一个线程池
        ExecutorService executorService = Executors.newCachedThreadPool();
        final int count = 50;
        //与countDownLatch.await();实现运行完所有线程之后才执行后面的操作
        CountDownLatch countDownLatch = new CountDownLatch(count);

        //创建100个线程
        for (int i = 0; i < count; i++) {
            HdfsPressThread target = new HdfsPressThread(i, countDownLatch);
            executorService.execute(target);
        }

    }

    @Override
    public void run() {
        try {
            getFs();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                countDownLatch.await();
                getFs().close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized FileSystem getFs() throws Exception {
        FileSystem fs;
        fs = FileSystem.get(uri, new Configuration(), "root");
        long begaintime = System.currentTimeMillis();//开始系统时间
        ContentSummary contentSummary = fs.getContentSummary(path);
        long actualSpace = contentSummary.getLength();
        long fileCount = contentSummary.getFileCount();
        //集群占用空间, 一般来说是实际占用空间的几倍, 具体与配置的副本数相关.
        long directoryCount = contentSummary.getDirectoryCount();
        long endTime = System.currentTimeMillis();//开始系统时间
        System.out.println("目录数使用：" + directoryCount + "  占用空间：" + actualSpace + "文件数：" + fileCount + "   耗时 ： " + (endTime - begaintime));
        return fs;
    }
}

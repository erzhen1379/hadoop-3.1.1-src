package org.apache.hadoop.hdfs.client.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsOpTest {
    public static void main(String[] args) throws IOException, InterruptedException {
        //链接集群NameNode地址
        URI uri = null;
        try {
            uri = new URI("hdfs://127.0.0.1:9000");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        //创建一个配置文件
        //    Configuration configuration = new Configuration();
//        设置集群副本数量为2
//        configuration.set("dfs.replication","2");
        //创建一个用户
        String user = "hadoop";
        FileSystem fs = FileSystem.get(uri, new Configuration(), user);
        for (int i=0;i<1000000;i++){
            String str="/press/hadoop_5_"+i;
            FSDataOutputStream fos = fs.create(new Path(str));
            fos.write("hello worldafaf".getBytes());
        }




        //去取一个文件到本地
        //  fs.copyToLocalFile(new Path("/hadoop-3.1.1.tar.gz"), new Path("/tmp/"));
        fs.close();
    }
}

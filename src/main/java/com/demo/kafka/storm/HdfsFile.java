package com.demo.kafka.storm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by afsar.khan on 2/6/18.
 */
public class HdfsFile {

    private static final String destinationLocation = "";//folder where you want to move the file
    private static final String sourceLocation = ""; //full path along with file name which you want to copy
    private static final String host = "hdfs://tbhortonctl1.int.iad2.xaxis.net:8020";

    public static void main(String[] args) throws Exception {

        copyToHDFS(destinationLocation, sourceLocation, false);

    }

    public static void copyToHDFS(String destinationLocation, String sourceLocation, boolean delSource) throws Exception {
        FileSystem fileSystem = null;
        try {
            org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(host + destinationLocation);
            org.apache.hadoop.fs.Path localPath = new org.apache.hadoop.fs.Path(sourceLocation);
            fileSystem = configureFileSystem(host);
            if (!fileSystem.isDirectory(hdfsPath)) {
                fileSystem.mkdirs(hdfsPath);
            }
            fileSystem.copyFromLocalFile(delSource, localPath, hdfsPath);
        } catch (IOException ex) {

        } finally {
            try {
                if (fileSystem != null) {
                    fileSystem.close();
                }
            } catch (IOException ex) {

            }
        }
    }


    private static FileSystem configureFileSystem(String host) throws IOException, URISyntaxException {
        FileSystem fileSystem = null;
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", host);
        fileSystem = FileSystem.get(new URI(host), configuration);
        return fileSystem;
    }
}

package edu.fiu.cs.yxjiang.event.util;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Put all the files in specified directory into specified HDFS location.
 * 
 * @author yjian004
 * 
 */
public class HDFSPutter {

  private static Configuration conf = new Configuration();

  /**
   * Copy all the files from local path into specified HDFS path
   * 
   * @param localPath
   * @param hdfsPath
   * @throws URISyntaxException
   * @throws IOException
   */
  public static void copyFile(String localPath, String hdfsPath)
      throws URISyntaxException, IOException {
    File local = new File(localPath);
    if (!local.exists()) {
      System.err.println("Local path does not exists.");
      System.exit(1);
    }

    if (!local.isDirectory()) {
      System.err.println("Given path is not a directory.");
      System.exit(1);
    }

    if (hdfsPath.indexOf(hdfsPath.length() - 1) != '/'
        || hdfsPath.indexOf(hdfsPath.length() - 1) != '\\')
      hdfsPath += '/';

    System.out.printf("Copy the following paths to %s\n", hdfsPath);
    URI uri = new URI(hdfsPath);
    FileSystem hdfs = FileSystem.get(uri, conf);
    for (File file : local.listFiles()) {
      if (!file.isFile() || file.getName().endsWith("log.log")
          || file.getName().endsWith("data.log"))
        continue;
      Path path = new Path(file.getAbsolutePath());
      System.out.printf("Copying %s...\n", path.getName());
      String hdfsFilePathStr = hdfsPath + file.getName();
      Path hdfsFilePath = new Path(hdfsFilePathStr);
      if (hdfs.exists(hdfsFilePath))
        continue;
      hdfs.copyFromLocalFile(true, false, path, hdfsFilePath);
    }

    System.out.println("Copy finished.");
  }

  public static void main(String[] args) throws URISyntaxException, IOException {
    if (args.length != 2) {
      System.err.println("usage: HDFSPutter localPath hdfsPath");
      System.exit(1);
    }
    String localPath = args[0];
    String hdfsPath = args[1];
    HDFSPutter.copyFile(localPath, hdfsPath);
  }

}

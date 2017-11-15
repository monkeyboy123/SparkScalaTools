package com.monkeyboy.java.hadoop.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopUtil {

	public static boolean exist(Path path,Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		return fs.exists(path);
	}

	public static boolean delete(Path path,Configuration conf) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		return fs.delete(path, true);
	}



}

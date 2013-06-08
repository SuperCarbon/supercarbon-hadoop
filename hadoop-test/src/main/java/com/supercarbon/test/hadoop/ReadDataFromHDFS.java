package com.supercarbon.test.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class ReadDataFromHDFS {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String url = "hdfs://localhost:9000/user/mj/input/core-site.xml";
		Configuration cfg = new Configuration();
		
		FSDataInputStream in = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(url), cfg);

			in = fs.open(new Path(url));
			IOUtils.copyBytes(in, System.out, 4096, false);
			

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}

	}

}

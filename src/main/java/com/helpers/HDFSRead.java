package com.helpers;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Reader HDFS
 */
public class HDFSRead {

	private Scanner scanner;
	Path pt; 

	/**
	 * @param pathname 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	public HDFSRead(String pathname) throws IOException, IllegalArgumentException {
		try {
			this.pt = new Path("hdfs:" + pathname);
			FileSystem fs = FileSystem.get(new Configuration());
			this.scanner = new Scanner(new InputStreamReader(fs.open(pt)));
		} catch(IOException e) {
			throw e;
		}
	}

	public String nextLine() throws NoSuchElementException {
		return scanner.nextLine();
	}

	public Boolean hasNextLine() {
		return this.scanner.hasNextLine();
	}
}

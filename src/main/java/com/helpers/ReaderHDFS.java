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
 * Ler arquivos contidos no HDFS.
 */
public class ReaderHDFS {

	private Scanner scanner; 

	/**
	 * @param pathname 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	public ReaderHDFS(String pathname) throws IOException, IllegalArgumentException {
		Path pt = new Path("hdfs:" + pathname);
		FileSystem fs = FileSystem.get(new Configuration());
		this.scanner = new Scanner(new InputStreamReader(fs.open(pt)));
	}

	public String nextLine() throws NoSuchElementException {
		return scanner.nextLine();
	}

	public Boolean hasNextLine() {
		return this.scanner.hasNextLine();
	}

	public void close() {
		this.scanner.close();
	}
}

package com.helpers;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.NoSuchElementException;
import java.util.Scanner;

/**
 * Wrapper para a classe Scanner
 */
public class ReadDataset {

	private File file;
	private Scanner scanner;

	public ReadDataset(String pathname) throws FileNotFoundException {
		this.file = new File(pathname);
		this.scanner = new Scanner(file);
	}

	public String nextLine() throws NoSuchElementException {
		return scanner.nextLine();
	}

	public Boolean hasNextLine() {
		return this.scanner.hasNextLine();
	}
}

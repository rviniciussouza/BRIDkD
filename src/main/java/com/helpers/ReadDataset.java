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
		try {
			this.file = new File(pathname);
			this.scanner = new Scanner(file);
		} catch(FileNotFoundException e) {
			throw new FileNotFoundException("Não foi possível localizar arquivo " + pathname);
		}
	}

	public String nextLine() throws NoSuchElementException {
		return scanner.nextLine();
	}

	public Boolean hasNextLine() {
		return this.scanner.hasNextLine();
	}
}

package com.helpers;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

import com.types.TupleWritable;
public class ReadDataset {

    private File file;
    private int number_tuples;
    private int dimension;
    private String name_dataset;
    private int attr_id;
    private int attr_description;
    private List<com.types.TupleWritable> tuples;
    private int number_line = 4;

    public ReadDataset(String pathname) throws Exception {
        file = new File(pathname);
//        this.read();
    }
    
    public List<String> getLines()  throws FileNotFoundException {
        List<String> lines = new ArrayList<>();
        try {
            Scanner scanner = new Scanner(file);
            while(scanner.hasNext()) {
                lines.add(scanner.nextLine());
            }
            scanner.close();
            return lines;
        }
        catch(FileNotFoundException e) {
            throw e;
        } 
    }

    public ReadDataset(Integer dimension, Integer attr_id, Integer attr_dimension) {
        this.dimension = dimension;
        this.attr_id = attr_id;
        this.attr_description = attr_dimension;
    }

    private void read() throws FileNotFoundException {
        try {
            Scanner scanner = new Scanner(file);
            this.readHeader(scanner);
            this.readTuples(scanner);
            scanner.close();
        }
        catch(FileNotFoundException e) {
            throw e;
        }
    }

    private void readHeader(Scanner scanner) {
        this.number_tuples = scanner.nextInt();
        this.attr_id = scanner.nextInt();
        this.dimension = scanner.nextInt();
        this.attr_description = scanner.nextInt();
        this.name_dataset = scanner.next();
    }

    private void readTuples(Scanner scanner) {
        scanner.nextLine();
        this.tuples = new ArrayList<>();
        while(scanner.hasNextLine()) {
            tuples.add(this.getTuple(scanner.nextLine()));
            number_line++;
        }
    }

    public TupleWritable getTuple(String line) {
        try {
            StringTokenizer st = new StringTokenizer(line);
            TupleWritable tuple = new TupleWritable();
            tuple.setRaw(line);
            if(this.attr_id == 1) tuple.setId(Integer.parseInt(st.nextToken()));
            for(int i = 0; i < this.dimension; i++) {
                tuple.addAttribute(Double.parseDouble(st.nextToken()));
            }
            if(this.attr_description == 1) tuple.setDescription(st.nextToken("\n"));
            return tuple;
        }
        catch(Exception e) {
            System.out.println(number_line);
            System.out.println(line);
        }
        return null;
    }


    public int getNumberTuples() {
        return this.number_tuples;
    }

    public int getDimension() {
        return this.dimension;
    }

    public String getNameDataset() {
        return this.name_dataset;
    }

    public List<TupleWritable> getTuples() {
        return this.tuples;
    }
}

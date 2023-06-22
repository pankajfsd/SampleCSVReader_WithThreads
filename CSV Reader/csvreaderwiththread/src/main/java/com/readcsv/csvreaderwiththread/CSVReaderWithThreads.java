package com.readcsv.csvreaderwiththread;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class CSVReaderWithThreads {
    private static final String CSV_FILE_PATH = "./src/main/java/SampleCSV_File.csv";
    private static final int NUM_THREADS = 5;

    public static void main(String[] args) {
        // Create a thread pool with the desired number of threads
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        // Read the CSV file and split it into chunks for each thread
        List<List<String>> csvChunks = splitCSVFile();

        // Process each chunk in parallel using threads
        for (List<String> chunk : csvChunks) {
            Runnable worker = new CSVReaderWorker(chunk);
            executor.execute(worker);
        }

        // Shutdown the executor and wait for all tasks to complete
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("CSV file reading completed.");
    }

    private static List<List<String>> splitCSVFile() {
        List<List<String>> csvChunks = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(CSV_FILE_PATH))) {
            String line;
            List<String> chunk = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                chunk.add(line);
                // Adjust the chunk size according to your requirements
                if (chunk.size() >= 1000) {
                    csvChunks.add(chunk);
                    chunk = new ArrayList<>();
                }
            }
            // Add the remaining lines as the last chunk
            if (!chunk.isEmpty()) {
                csvChunks.add(chunk);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return csvChunks;
    }

    static class CSVReaderWorker implements Runnable {
        private final List<String> csvChunk;

        public CSVReaderWorker(List<String> csvChunk) {
            this.csvChunk = csvChunk;
        }

        public void run() {
            // Process the CSV chunk
            for (String line : csvChunk) {
                // Perform your desired operations on each line
                System.out.println(line);
            }
        }
    }
}

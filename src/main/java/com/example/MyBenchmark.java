 /*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.example;
import org.openjdk.jmh.annotations.*;
import java.sql.*;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.ArrayList;

@Fork(value = 3)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)

public class MyBenchmark {
    private static final String URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USER = "oleg";
    private static final String PASSWORD = "";
    
    @Param({"5"})  
    private int scale;
    
    private Connection connection;
    private Random random;
    private int nAccounts;
    private int nTellers;
    private int nBranches;
    
    @Setup(Level.Trial)
    public void setup() throws SQLException {
        connection = DriverManager.getConnection(URL, USER, PASSWORD);
        connection.setAutoCommit(false);
        random = new Random();
        
        // Calculate counts based on pgbench scale
        nAccounts = 100000 * scale;
        nTellers = 10 * scale;
        nBranches = 1 * scale;
    }
    
    @TearDown(Level.Trial)
    public void teardown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
    
    @Benchmark
    public int selectOnlyBenchmark() throws SQLException {
        int aid = random.nextInt(nAccounts) + 1;
        PreparedStatement pstmt = connection.prepareStatement(
            "SELECT abalance FROM pgbench_accounts WHERE aid = ?"
        );
        pstmt.setInt(1, aid);
        ResultSet rs = pstmt.executeQuery();
        int balance = 0;
        if (rs.next()) {
            balance = rs.getInt(1);
        }
        rs.close();
        pstmt.close();
        return balance;
    }
    
    // Main method for latency collection mode
    public static void main(String[] args) {
        String outputFile = args.length > 0 ? args[0] : "pgbench_results.csv";
        int runNumber = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        int scaleParam = args.length > 2 ? Integer.parseInt(args[2]) : 5;
        
        System.out.println("PostgreSQL pgbench Select-Only Benchmark");
        System.out.println("Run #" + runNumber + ", Scale: " + scaleParam);
        
        boolean fileExists = new java.io.File(outputFile).exists();
        
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile, true))) {
            if (!fileExists) {
                writer.println("run,iteration,latency_ns");
            }
            
            MyBenchmark benchmark = new MyBenchmark();
            benchmark.scale = scaleParam;
            benchmark.setup();
            
            // Warmup (no data collection)
            int warmupIterations = 5000;
            System.out.println("Warmup: " + warmupIterations + " iterations");
            for (int i = 0; i < warmupIterations; i++) {
                try {
                    benchmark.selectOnlyBenchmark();
                } catch (SQLException e) {
                    System.err.println("Error in warmup: " + e.getMessage());
                }
            }
            
            // Measurement phase
            int benchmarkIterations = 1_000_000;
            System.out.println("Measurement: " + benchmarkIterations + " iterations");
            List<Long> latencies = new ArrayList<>();
            
            for (int i = 0; i < benchmarkIterations; i++) {
                long start = System.nanoTime();
                try {
                    benchmark.selectOnlyBenchmark();
                } catch (SQLException e) {
                    System.err.println("Error in iteration " + i + ": " + e.getMessage());
                    continue;
                }
                long latency = System.nanoTime() - start;
                latencies.add(latency);
                writer.println(runNumber + "," + i + "," + latency);
            }
            
            benchmark.teardown();
            
            // Calculate statistics
            latencies.sort(Long::compareTo);
            double avg = latencies.stream().mapToLong(l -> l).average().orElse(0.0);
            long p50 = latencies.get((int)(latencies.size() * 0.50));
            long p95 = latencies.get((int)(latencies.size() * 0.95));
            long p99 = latencies.get((int)(latencies.size() * 0.99));
            
            System.out.println("\n=== Results ===");
            System.out.println("Avg: " + String.format("%.2f", avg / 1000) + " μs");
            System.out.println("P50: " + String.format("%.2f", p50 / 1000.0) + " μs");
            System.out.println("P95: " + String.format("%.2f", p95 / 1000.0) + " μs");
            System.out.println("P99: " + String.format("%.2f", p99 / 1000.0) + " μs");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

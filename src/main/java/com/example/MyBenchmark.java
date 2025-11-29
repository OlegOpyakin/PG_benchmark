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
}

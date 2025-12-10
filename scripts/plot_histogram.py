#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import os


def plot_histogram(csv_file, output_dir='results'):
    print(f"Reading {csv_file}...")
    df = pd.read_csv(csv_file)
    
    if df.empty:
        print("No data found")
        return
    
    print(f"Loaded {len(df)} measurements")
    os.makedirs(output_dir, exist_ok=True)
    
    df['latency_us'] = df['latency_ns'] / 1000.0
    
    # Calculate statistics on full dataset
    p50 = df['latency_us'].quantile(0.50)
    p95 = df['latency_us'].quantile(0.95)
    p99 = df['latency_us'].quantile(0.99)
    mean = df['latency_us'].mean()
    
    print(f"\nStatistics (μs):")
    print(f"  Mean: {mean:.2f}")
    print(f"  P50:  {p50:.2f}")
    print(f"  P95:  {p95:.2f}")
    print(f"  P99:  {p99:.2f}")
    
    # Filter outliers for plotting (keep 99.9%)
    p999 = df['latency_us'].quantile(0.995)
    df_plot = df[df['latency_us'] <= p999]
    outliers_removed = len(df) - len(df_plot)
    if outliers_removed > 0:
        print(f"\nFiltered {outliers_removed} outliers (>{p999:.2f} μs) for visualization")
    
    # Histogram with more bins
    fig, ax = plt.subplots(figsize=(12, 7))
    n_bins = min(200, max(100, int(np.sqrt(len(df_plot))) * 2))
    
    ax.hist(df_plot['latency_us'], bins=n_bins, color='skyblue', edgecolor='black', alpha=0.7, linewidth=0.5)
    
    # Percentile lines
    ax.axvline(mean, color='green', linestyle='--', linewidth=1.5, label=f'Mean: {mean:.2f} μs')
    ax.axvline(p50, color='blue', linestyle='--', linewidth=1.5, label=f'P50: {p50:.2f} μs')
    ax.axvline(p95, color='orange', linestyle='--', linewidth=1.5, label=f'P95: {p95:.2f} μs')
    ax.axvline(p99, color='red', linestyle='--', linewidth=1.5, label=f'P99: {p99:.2f} μs')
    
    ax.set_xlabel('Latency (μs)', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('PostgreSQL pgbench Select-Only Latency Distribution', fontsize=14, fontweight='bold')
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_file = os.path.join(output_dir, 'latency_histogram.png')
    plt.savefig(output_file, dpi=300)
    print(f"\nSaved: {output_file}")
    plt.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python plot_histogram.py <csv_file> [output_dir]")
        print("Example: python plot_histogram.py results/pgbench_results.csv results")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else 'results'
    
    if not os.path.exists(csv_file):
        print(f"Error: CSV file not found: {csv_file}")
        sys.exit(1)
    
    plot_histogram(csv_file, output_dir)
    print("\nHistogram generation complete!")

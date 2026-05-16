import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import pandas as pd
import os

# Set the cwd to parent
os.chdir(os.path.join(os.path.dirname(__file__), os.pardir))
MEDIA_DST = Path.cwd() / 'plots'

if __name__ == '__main__':
  
  # Node range 3...10
  node_count = list(range(3,11))
  key_ranges = [1000,2000,4000,8000,16000,32000,64000,128000]
  
  exp_1 = pd.read_csv("results/exp_1.csv")
  lat_avg_rep1 = exp_1[exp_1['replication_degree'] == 1]['lat_us_avg'].tolist()
  lat_avg_rep2 = exp_1[exp_1['replication_degree'] == 2]['lat_us_avg'].tolist()
  lat_avg_rep3 = exp_1[exp_1['replication_degree'] == 3]['lat_us_avg'].tolist()
  
  lat_p50_rep1 = exp_1[exp_1['replication_degree'] == 1]['lat_us_p50'].tolist()
  lat_p50_rep2 = exp_1[exp_1['replication_degree'] == 2]['lat_us_p50'].tolist()
  lat_p50_rep3 = exp_1[exp_1['replication_degree'] == 3]['lat_us_p50'].tolist()
  
  lat_p90_rep1 = exp_1[exp_1['replication_degree'] == 1]['lat_us_p90'].tolist()
  lat_p90_rep2 = exp_1[exp_1['replication_degree'] == 2]['lat_us_p90'].tolist()
  lat_p90_rep3 = exp_1[exp_1['replication_degree'] == 3]['lat_us_p90'].tolist()
  
  lat_p90_99_rep1 = exp_1[exp_1['replication_degree'] == 1]['lat_us_p99'].tolist()
  lat_p90_99_rep2 = exp_1[exp_1['replication_degree'] == 2]['lat_us_p99'].tolist()
  lat_p90_99_rep3 = exp_1[exp_1['replication_degree'] == 3]['lat_us_p99'].tolist()
  
  exp_2 = pd.read_csv("results/exp_2.csv")
  # exp_3 = pd.read_csv("results/exp_3.csv")

  # Vary key ranges: 
  plt.figure()
  plt.plot(key_ranges, lat_avg_rep1, label='rep-degree=1', marker='o', color='blue')
  plt.plot(key_ranges, lat_avg_rep2, label='rep-degree=2', marker='o', color='green')
  plt.plot(key_ranges[:-1], lat_avg_rep3, label='rep-degree=3', marker='o', color='red')
  plt.xlabel('Key Range')
  plt.ylabel('Average Latency (us)')
  plt.title('Average Latency vs Key Range')
  plt.xscale('log')

  plt.xticks(key_ranges, labels=['1k','2k','4k','8k','16k','32k','64k','128k'])
  plt.grid()
  plt.legend()
  plt.savefig(MEDIA_DST / 'lat_avg_key_range.png')
  
  plt.figure()
  plt.plot(key_ranges, lat_p50_rep1, label='rep-degree=1', marker='o', color='blue')
  plt.plot(key_ranges, lat_p50_rep2, label='rep-degree=2', marker='o', color='green')
  plt.plot(key_ranges[:-1], lat_p50_rep3, label='rep-degree=3', marker='o', color='red')
  plt.xlabel('Key Range')
  plt.ylabel('50th Percentile Latency (us)')
  plt.title('50th Percentile Latency vs Key Range')
  plt.xscale('log')

  plt.xticks(key_ranges, labels=['1k','2k','4k','8k','16k','32k','64k','128k'])
  plt.grid()
  plt.legend()
  plt.savefig(MEDIA_DST / 'lat_p50_key_range.png')
  
  plt.figure()
  plt.plot(key_ranges, lat_p90_rep1, label='rep-degree=1', marker='o', color='blue')
  plt.plot(key_ranges, lat_p90_rep2, label='rep-degree=2', marker='o', color='green')
  plt.plot(key_ranges[:-1], lat_p90_rep3, label='rep-degree=3', marker='o', color='red')
  plt.xlabel('Key Range')
  plt.ylabel('90th Percentile Latency (us)')
  plt.title('90th Percentile Latency vs Key Range')
  plt.xscale('log')

  plt.xticks(key_ranges, labels=['1k','2k','4k','8k','16k','32k','64k','128k'])
  plt.grid()
  plt.legend()
  plt.savefig(MEDIA_DST / 'lat_p90_key_range.png')
  
  plt.figure()
  plt.plot(key_ranges, lat_p90_99_rep1, label='rep-degree=1', marker='o', color='blue')
  plt.plot(key_ranges, lat_p90_99_rep2, label='rep-degree=2', marker='o', color='green')
  plt.plot(key_ranges[:-1], lat_p90_99_rep3, label='rep-degree=3', marker='o', color='red')
  plt.xlabel('Key Range')
  plt.ylabel('99th Percentile Latency (us)')
  plt.title('99th Percentile Latency vs Key Range')
  plt.xscale('log')

  plt.xticks(key_ranges, labels=['1k','2k','4k','8k','16k','32k','64k','128k'])
  plt.grid()
  plt.legend()
  plt.savefig(MEDIA_DST / 'lat_p99_key_range.png')
  
  fig, axes = plt.subplots(2, 2, figsize=(14, 10))

  data = [
      (lat_avg_rep1, lat_avg_rep2, lat_avg_rep3, 'Average Latency (us)',         'Average Latency vs Key Range'),
      (lat_p50_rep1, lat_p50_rep2, lat_p50_rep3, '50th Percentile Latency (us)', 'P50 Latency vs Key Range'),
      (lat_p90_rep1, lat_p90_rep2, lat_p90_rep3, '90th Percentile Latency (us)', 'P90 Latency vs Key Range'),
      (lat_p90_99_rep1, lat_p90_99_rep2, lat_p90_99_rep3, '99th Percentile Latency (us)', 'P99 Latency vs Key Range'),
  ]
  
  key_ranges = [1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000]
  key_labels = ['1k', '2k', '4k', '8k', '16k', '32k', '64k', '128k']

  for ax, (rep1, rep2, rep3, ylabel, title) in zip(axes.flat, data):
      ax.plot(key_ranges,             rep1, marker='o', label='rep-degree=1', color='blue')
      ax.plot(key_ranges,             rep2, marker='o', label='rep-degree=2', color='green')
      ax.plot(key_ranges[:len(rep3)], rep3, marker='o', label='rep-degree=3', color='red')
      ax.set_xlabel('Key Range')
      ax.set_ylabel(ylabel)
      ax.set_title(title)
      ax.set_xscale('log')
      ax.set_xticks(key_ranges)
      ax.set_xticklabels(key_labels)
      ax.grid()
      ax.legend()

  plt.tight_layout()
  plt.savefig(MEDIA_DST / 'lat_key_range_combined.png')
  plt.close()
  
  # latency vs node count
  exp_2 = pd.read_csv("results/exp_2.csv")
  lat_avg = exp_2['lat_us_avg'].tolist()
  lat_p50 = exp_2['lat_us_p50'].tolist()
  lat_p90 = exp_2['lat_us_p90'].tolist()
  lat_p90_99 = exp_2['lat_us_p99'].tolist()
  
  plt.figure()
  plt.plot(node_count, lat_avg, label='Avg.', marker='o', color='blue')
  plt.plot(node_count, lat_p50, label='p50', marker='o', color='green')
  plt.plot(node_count, lat_p90, label='p90', marker='o', color='red')
  plt.plot(node_count, lat_p90_99, label='p99.9', marker='o', color='orange')

  plt.xlabel('Node Count')
  plt.ylabel('Latency (us)')
  plt.title('Latency vs Node Count')
  plt.xscale('log')

  plt.xticks(node_count, labels=[str(n) for n in node_count])
  plt.grid()
  plt.legend()
  plt.savefig(MEDIA_DST / 'lat_node_count.png')
  
  # throughput vs node count
  exp_2 = pd.read_csv("results/exp_2.csv")
  thru_ops_s = exp_2['thru_avg_ops_s'].tolist()
  
  plt.figure()
  plt.plot(node_count, thru_ops_s, label='Throughput', marker='o', color='blue')

  plt.xlabel('Node Count')
  plt.ylabel('Throughput (ops/s)')
  plt.title('Throughput vs Node Count')
  plt.xscale('log')

  plt.xticks(node_count, labels=[str(n) for n in node_count])
  plt.grid()
  plt.legend()
  plt.savefig(MEDIA_DST / 'thru_node_count.png')
  
  # election latency vs node count
  
  exp_2 = pd.read_csv("results/exp_2.csv")
  thru_ops_s = exp_2['election_lat'].tolist()
  
  plt.figure()
  plt.plot(node_count, thru_ops_s, label='Election Latency', marker='o', color='blue')

  plt.xlabel('Node Count')
  plt.ylabel('Election Latency (us)')
  plt.title('Election Latency vs Node Count')
  plt.xscale('log')

  plt.xticks(node_count, labels=[str(n) for n in node_count])
  plt.grid()
  plt.legend()
  plt.savefig(MEDIA_DST / 'election_lat_node_count.png')
  
  # failover histogram 
  results = pd.read_csv("results/failover.csv")
  data = results['lat_s'].values
  
  # convert to microseconds
  data = data * 1e6
  # remove anything over 100,000 lat
  data = data[data < 100000]
  
  mean_val = np.mean(data)
  median_val = np.median(data)
  std_val = np.std(data)
  p50 = np.percentile(data, 50)
  p95 = np.percentile(data, 95)
  p99 = np.percentile(data, 99)
  min_val = np.min(data)
  max_val = np.max(data)
  
  print(f"Statistics for {len(data)} samples:")
  print(f"  Mean:   {mean_val:.2f} µs")
  print(f"  Median: {median_val:.2f} µs")
  print(f"  Std:    {std_val:.2f} µs")
  print(f"  Min:    {min_val:.2f} µs")
  print(f"  Max:    {max_val:.2f} µs")
  print(f"  p50:    {p50:.2f} µs")
  print(f"  p95:    {p95:.2f} µs")
  print(f"  p99:    {p99:.2f} µs")
  
  # Create figure
  fig, ax = plt.subplots(figsize=(10, 6))
  
  # Determine bin width (Freedman-Diaconis rule)
  q75, q25 = np.percentile(data, [75, 25])
  iqr = q75 - q25
  bin_width = 2 * iqr / (len(data) ** (1/3))
  n_bins = int(np.ceil((max_val - min_val) / bin_width))
  n_bins = max(20, min(n_bins, 100))  # Clamp between 20 and 100 bins
  n_bins = 15
  
  # Plot histogram
  counts, bins, patches = ax.hist(data, bins=n_bins, 
                                    color='steelblue', 
                                    alpha=0.7, 
                                    edgecolor='black',
                                    linewidth=0.5)
  
  # Add vertical lines for percentiles
  ax.axvline(median_val, color='red', linestyle='--', linewidth=2, 
              label=f'Median: {median_val:.1f} µs')
  ax.axvline(p95, color='orange', linestyle='--', linewidth=2, 
              label=f'p95: {p95:.1f} µs')
  ax.axvline(p99, color='darkred', linestyle='--', linewidth=2, 
              label=f'p99: {p99:.1f} µs')
  
  # Labels and title
  ax.set_xlabel('Failover Latency (µs)', fontsize=12, fontweight='bold')
  ax.set_ylabel('Frequency', fontsize=12, fontweight='bold')
  ax.set_title(f'Failover Latency Distribution (n={len(data)})', 
                fontsize=14, fontweight='bold')
  
  # Add statistics text box
  stats_text = f'Mean: {mean_val:.1f} µs\n'
  stats_text += f'Std: {std_val:.1f} µs\n'
  stats_text += f'Min: {min_val:.1f} µs\n'
  stats_text += f'Max: {max_val:.1f} µs'
  
  ax.text(0.98, 0.97, stats_text,
          transform=ax.transAxes,
          fontsize=10,
          verticalalignment='top',
          horizontalalignment='right',
          bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
  
  # Legend
  ax.legend(loc='upper right', fontsize=10, framealpha=0.9)
  
  # Grid
  ax.grid(True, alpha=0.3, linestyle='--')
  
  # Tight layout
  plt.tight_layout()
  
  output= MEDIA_DST / 'failover.png'
  
  # Save
  plt.savefig(output, dpi=300, bbox_inches='tight')
  print(f"\nPlot saved to: {output}")
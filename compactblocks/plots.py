import matplotlib as mpl
import matplotlib.pyplot as plt
import mplcursors
import numpy as np
import seaborn as sns


font = {
    'family': 'serif',
    'color': '#02082e',
    'weight': 'normal',
    'size': 12,
}
mpl.rcParams['font.weight'] = 'normal'
mpl.rcParams['font.family'] = 'serif'
plt.ion()


# Make violinplot of received CMPCTBLOCK's.
def plot_received_size(prefiller_received):
    prefiller_received['were_bytes_missing?'] = np.where(prefiller_received['bytes_missing'] > 0, 'Yes', 'No')
    pallette_color = "#749993"
    f, ax1 = plt.subplots()

    sns.violinplot(data=prefiller_received, x='received_size', hue='were_bytes_missing?', palette=f'light:{pallette_color}',
                   ax=ax1, gap=0.2)
    xticks_bytes = [1024, 10*1024, 20*1024, 30*1024]
    xtick_labels = ['1 KiB', '10 KiB', '20 KiB', '30 KiB']
    ax1.xaxis.set_major_formatter(mpl.ticker.ScalarFormatter())
    ax1.set_xticks(xticks_bytes)
    ax1.set_xticklabels(xtick_labels)

    ax1.set_title('Received CMPCTBLOCK sizes', fontdict=font)
    ax1.xaxis.set_label_text('Received size in KiB')
    plt.show(block=True)


# Plot reconstruction time histogram and bytesmissing/reconstructiontime scatterplot
def plot_reconstruction_histogram_and_scatterplot(received, title):
    histcolor = "#749940"
    received['reconstruction_time_ms'] = received['reconstruction_time_ns'] / 1_000_000
    received['were_bytes_missing?'] = np.where(received['bytes_missing'] > 0, 'Yes', 'No')

    f, [ax1, ax2] = plt.subplots(2, figsize=(10,10), tight_layout=True)
    # Histplot on top
    sns.histplot(data=received, x='reconstruction_time_ms', hue='were_bytes_missing?', ax=ax1, color=histcolor, log_scale=2, bins=70)
    ax1.xaxis.set_major_formatter(mpl.ticker.ScalarFormatter())
    ax1.set_xticks([0.5, 1, 5, 50, 200, 1000])
    ax1.set_title(f'Histogram of reconstruction times for {title}', fontdict=font)
    ax1.xaxis.set_label_text('Reconstruction time (ms) [$log_{2}$ scale]')

    # Scatterplot on bottom "
    scattercolor = "#80b895"
    xticks = [1, 10, 50, 100, 1000]
    yticks_bytes = [0, 256, 1024, 10*1024, 1024*1024, 4*1024*1024] # 256B, 1KB, 10KB, 100KB, 1MB, 4MB
    ytick_labels = ['0 B', '256 B', '1 KiB', '10 KiB', '1 MiB', '4 MiB']
    sns.scatterplot(data=received, x='reconstruction_time_ms', y='bytes_missing',
                    ax=ax2, color=scattercolor, alpha=0.3, s=30)
    sns.rugplot(data=received, x='reconstruction_time_ms', y='bytes_missing',
                ax=ax2, color=scattercolor, alpha=0.1)

    ax2.set_title(f'Scatter plot of Missing Bytes/Reconstruction Time for {title}', fontdict=font)
    ax2.set_xlabel('Reconstruction Time (ms) [$log_{10}$ scale]')
    ax2.set_xticks(xticks)
    ax2.set_xscale('log') 
    ax2.set_xlim(left=1)
    ax2.xaxis.set_major_formatter(mpl.ticker.ScalarFormatter())

    ax2.yaxis.set_major_formatter(mpl.ticker.ScalarFormatter())
    ax2.set_ylim(-0.5)
    ax2.set_ylabel('Missing Bytes ($log_10$ scale)')
    ax2.set_yscale('symlog')
    ax2.set_yticks(yticks_bytes)
    ax2.set_yticklabels(ytick_labels)
    plt.show(block=True)


# Plot TCP Window Histogram
def plot_tcp_window_histogram(sent):
    histcolor = "#040920"

    f, ax1 = plt.subplots()
    # Histplot on top
    xticks_bytes = [1024, 10*1024, 100*1024, 1024*1024, 4*1024*1024] # 256B, 1KB, 10KB, 100KB, 1MB, 4MB
    xtick_labels = ['1KiB','10KiB', '100KiB', '1MiB', '4MiB']
    sns.histplot(data=sent, x='tcp_window_size', ax=ax1, color=histcolor, log_scale=10, bins=100)
    ax1.set_title(r"Histogram of TCP Window Sizes for sent CMPCTBLOCK's. [$log_{10}$ scale]", fontdict=font)
    ax1.xaxis.set_major_formatter(mpl.ticker.ScalarFormatter())
    ax1.xaxis.set_label_text('TCP Window Size in KiB ($log_{10}$ scale)')
    ax1.set_xticks(xticks_bytes)
    ax1.set_xticklabels(xtick_labels)
    ax1.set_xlim([1024, 4*1024*1024])
    ax1.set_yscale('log')
    ax1.yaxis.set_major_formatter(mpl.ticker.ScalarFormatter())
    ax1.yaxis.set_label_text('Count [$log_{10}$ scale]')
    plt.show(block=True)


# Plot distribution of prefill sizes
def plot_prefill_distributions(sent):
    prefilled_sends = sent[sent['prefill_size'] > 0]

    xticks = [256, 512, 1024, 2*1024, 4*1024, 10*1024, 100*1024, 1024*1024, 4*1024*1024]
    xtick_labels = ['256B', '512B', '1KiB', '2KiB', '4KiB', '10KiB', '100KiB', '1MiB', '4MiB']
    # Special red vertical line for the average available space for prefill
    avg_prefill_size = prefilled_sends['window_bytes_available'].mean()
    avg_prefill_line = {
        'x': avg_prefill_size,
        'color': '#9c2020',
        'alpha': 0.8,
        'linestyle': '-',
        'linewidth': 2,
        'label': f"Avg Available Bytes in Window: {avg_prefill_size:.0f} bytes",
    }

    fig, (ax1, ax2) = plt.subplots(2, figsize=(10, 8), tight_layout=True)
    sns.ecdfplot(data=prefilled_sends, x="prefill_size", log_scale=2, stat='percent', ax=ax1)
    ax1.set_title('Cumulative distribution of prefill sizes', fontdict=font)
    ax1.set_xticks(xticks)
    # Draw vertical lines at each tick
    for xtick in xticks:
        ax1.axvline(x=xtick, color='gray', linestyle='--', alpha=0.4)

    ax1.xaxis.set_label_text("Prefill size (bytes) [$log_{2}$] scale")
    ax1.set_xticklabels(xtick_labels)

    ax1.axvline(**avg_prefill_line)
    ax1.legend()

    sns.histplot(data=prefilled_sends, x="prefill_size", log_scale=2, ax=ax2, kde=True)
    ax2.set_title('Histogram of prefill sizes', fontdict=font)
    ax2.set_xticks(xticks)
    ax2.set_xticklabels(xtick_labels)
    ax2.xaxis.set_label_text("Prefill size (bytes) [$log_{2}$] scale")

    ax2.axvline(**avg_prefill_line)
    ax2.legend()
    mplcursors.cursor([ax1, ax2], hover=True)

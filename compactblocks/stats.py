import pandas as pd


# Received CMPCTBLOCK stats
def received_stats(received: pd.DataFrame) -> None:
    total_cb_received = len(received)
    if total_cb_received == 0:
        return
    failed_blocks = received[received['received_tx_missing'] > 0]
    fail_rate = len(failed_blocks) / total_cb_received
    print(f"{len(failed_blocks)} out of {total_cb_received} blocks received failed reconstruction. ({fail_rate * 100:.2f}%)")
    reco_rate = 1 - fail_rate
    print(f"Reconstruction rate was {reco_rate * 100:.2f}%")

    avg_received_size = received['received_size'].mean()
    print(f"Avg size of received block: {avg_received_size:.2f} bytes")

    avg_missing_tx_size = received['bytes_missing'].mean()
    print(f"Avg bytes missing from received blocks: {avg_missing_tx_size:.2f} bytes")

    avg_missing_from_failed = failed_blocks['bytes_missing'].mean()
    print(f"Avg bytes missing from blocks that failed reconstruction: {avg_missing_from_failed:.2f} bytes")

    avg_reco_time = (received['time_reconstructed'] - received['time_received']).mean()
    avg_reco_time_in_ms = avg_reco_time.value / (1000 * 1000)
    print(f"Avg reconstruction time: {avg_reco_time_in_ms}ms")


# SENT CMPCTBLOCK stats
def sent_stats(sent: pd.DataFrame) -> None:
    avg_send_size = sent['send_size'].mean()
    print(f"The average CMPCTBLOCK we sent was {avg_send_size:.2f} bytes.")
    prefilled_sends = sent[sent['prefill_size'] > 0]
    print(f"The average prefilled CMPCTBLOCK we sent was {prefilled_sends['send_size'].mean():.2f} bytes.")
    not_prefilled_sends = sent[sent['prefill_size'] == 0]
    print(f"The average prefilled CMPCTBLOCK we sent was {not_prefilled_sends['send_size'].mean():.2f} bytes.")

    total_cb_sent = len(sent)

    avg_available_bytes_all = sent['window_bytes_available'].mean()

    prefilled_sends = sent[sent['prefill_size'] > 0]
    total_prefilled_cb_sent = len(prefilled_sends)
    prefill_needed_rate = total_prefilled_cb_sent / total_cb_sent
    print(f"{total_prefilled_cb_sent}/{total_cb_sent} blocks were sent with prefills. ({prefill_needed_rate * 100:.2f}%)")
    print(f"Avg available prefill bytes for all CMPCTBLOCK's we sent: {avg_available_bytes_all:.2f} bytes")

    # At this point, we return if this is not a prefilling node.
    if total_prefilled_cb_sent == 0:
        return

    avg_prefill_bytes = prefilled_sends['prefill_size'].mean()
    prefills_that_fit = (prefilled_sends['prefill_size'] <= prefilled_sends['window_bytes_available']).sum()
    prefill_fit_rate = prefills_that_fit / total_prefilled_cb_sent
    avg_available_bytes_for_needed = prefilled_sends['window_bytes_available'].mean()

    print(f"Avg available prefill bytes for prefilled CMPCTBLOCK's we sent: {avg_available_bytes_for_needed:.2f} bytes")
    print(f"Avg total prefill size for CMPCTBLOCK's we prefilled: {avg_prefill_bytes:.2f} bytes")

    print(f"{prefills_that_fit}/{total_prefilled_cb_sent} prefilled blocks sent fit in the available bytes. ({prefill_fit_rate * 100:.2f}%)")


def sent_window_stats(sent: pd.DataFrame) -> None:
    window_sizes = sent['tcp_window_size']
    print(f"TCP Window Size: Avg: {window_sizes.mean():.2f} bytes, Median: {window_sizes.median()}, Mode: {window_sizes.mode()[0]}")
    mode_freq = (window_sizes == window_sizes.mode()[0]).sum()
    print(f"The mode represented {mode_freq}/{len(window_sizes)} windows. ({mode_freq/len(window_sizes)*100:.2f}%)")
    avg_window_used = sent['window_bytes_used'].mean()
    print(f"Avg. TCP window bytes used: {avg_window_used:.2f} bytes")
    avg_window_available = sent['window_bytes_available'].mean()
    print(f"Avg. TCP window bytes available: {avg_window_available:.2f} bytes")

def sent_already_over_stats(sent: pd.DataFrame) -> None:
    total_cb_sent = len(sent)

    excessive = sent[sent['rtts_without_prefill'] > 1]
    already_over_rtt_rate = len(excessive) / total_cb_sent if total_cb_sent > 0 else 0
    print(f"{len(excessive)}/{total_cb_sent} CMPCTBLOCK's sent were already over the window for a single RTT before prefilling. ({already_over_rtt_rate * 100:.2f}%)")

    avg_available_bytes_in_exceeded = excessive['window_bytes_available'].mean()
    print(f"Avg. available bytes for prefill in blocks that were already over a single RTT: {avg_available_bytes_in_exceeded:.2f} bytes")
    excessive_that_fit = excessive['prefill_size'] <= excessive['window_bytes_available']
    excessive_fit_rate = len(excessive_that_fit) / len(excessive)
    print(f"{len(excessive_that_fit)}/{len(excessive)} excessively large blocks had prefills that fit. ({excessive_fit_rate * 100:.2f}%)")

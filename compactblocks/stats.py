import pandas as pd

# Received CMPCTBLOCK stats
def received_stats(received: pd.DataFrame) -> None:
    total_cb_received = len(received)
    if total_cb_received == 0:
        return
    failed_blocks = (received['received_tx_missing'] > 0).sum()
    fail_rate = failed_blocks / total_cb_received
    print(f"{failed_blocks} out of {total_cb_received} blocks received failed reconstruction. ({fail_rate * 100:.2f}%)")
    reco_rate = 1 - fail_rate
    print(f"Reconstruction rate was {reco_rate * 100:.2f}%")

# SENT CMPCTBLOCK stats
def sent_stats(sent: pd.DataFrame) -> None:
    total_cb_sent = len(sent)

    avg_available_bytes_all = sent['window_bytes_available'].mean()

    prefilled_sends = sent[sent['prefill_size'] > 0]
    total_prefilled_cb_sent = len(prefilled_sends)
    prefill_needed_rate = total_prefilled_cb_sent / total_cb_sent
    print(f"{total_prefilled_cb_sent}/{total_cb_sent} blocks were sent with prefills:. ({prefill_needed_rate * 100:.2f}%)")
    print(f"Avg available prefill bytes for all CMPCTBLOCK's we sent: {avg_available_bytes_all:.2f}")

    # At this point, we return if this is not a prefilling node. 
    if total_prefilled_cb_sent == 0:
        return

    avg_prefill_bytes = prefilled_sends['prefill_size'].mean()
    prefills_that_fit = (prefilled_sends['prefill_size'] <= prefilled_sends['window_bytes_available']).sum()
    prefill_fit_rate = prefills_that_fit / total_prefilled_cb_sent
    avg_available_bytes_for_needed = prefilled_sends['window_bytes_available'].mean()

    print(f"Avg available prefill bytes for prefilled CMPCTBLOCK's we sent: {avg_available_bytes_for_needed:.2f}")
    print(f"Avg total prefill size for CMPCTBLOCK's we prefilled: {avg_prefill_bytes:.2f}")

    print(f"{prefills_that_fit}/{total_prefilled_cb_sent} prefilled blocks sent fit in the available bytes. ({prefill_fit_rate * 100:.2f}%)")

# vExtraTxnForCompact stats
def sent_extra_txn_stats(sent: pd.DataFrame) -> None:
    prefilled_sends = sent[sent['prefill_size'] > 0]
    total_prefilled_cb_sent = len(prefilled_sends)
    # At this point, we return if this is not a prefilling node. 
    if total_prefilled_cb_sent == 0:
        return

    avg_prefill_bytes = prefilled_sends['prefill_size'].mean()
    avg_extra_prefill_bytes = prefilled_sends['extra_pool_prefill_size'].mean()
    avg_prefill_without_extra = avg_prefill_bytes - avg_extra_prefill_bytes
    print(f"Avg bytes of prefill that were from vExtraTxn: {avg_extra_prefill_bytes:.2f}")
    print(f"Avg prefill size w/o vExtraTxn's: {avg_prefill_without_extra:.2f}")
    
    no_extra_prefills_that_fit_count = ((prefilled_sends['prefill_size'] - prefilled_sends['extra_pool_prefill_size']) <= prefilled_sends['window_bytes_available']).sum()
    no_extra_prefills_that_fit_rate = no_extra_prefills_that_fit_count / total_prefilled_cb_sent
    print(f"{no_extra_prefills_that_fit_count}/{total_prefilled_cb_sent} prefilled blocks would have fit if we hadn't prefilled extra txn's. ({no_extra_prefills_that_fit_rate * 100:.2f}%)")

def sent_already_over_stats(sent: pd.DataFrame) -> None:
    total_cb_sent = len(sent)

    excessive = sent[sent['rtts_without_prefill'] > 1]
    already_over_rtt_rate = len(excessive) / total_cb_sent if total_cb_sent > 0 else 0
    print(f"{len(excessive)}/{total_cb_sent} CMPCTBLOCK's sent were already over the window for a single RTT before prefilling. ({already_over_rtt_rate * 100:.2f}%)")

    avg_available_bytes_in_exceeded = excessive['window_bytes_available'].mean()
    print(f"Avg. available bytes for prefill in blocks that were already over a single RTT: {avg_available_bytes_in_exceeded:.2f}")
    excessive_that_fit = excessive['prefill_size'] <= excessive['window_bytes_available']
    excessive_fit_rate = len(excessive_that_fit) / len(excessive)
    print(f"{len(excessive_that_fit)}/{len(excessive)} excessively large blocks had prefills that fit. ({excessive_fit_rate * 100:.2f}%)")

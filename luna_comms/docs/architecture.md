# Luna_Comms Architecture

Pipeline:

1. Detect source events from export/push artifacts.
2. Prepare channel-specific outbound jobs.
3. Queue ready jobs.
4. Send and record channel receipts.
5. Acknowledge and archive.

All transitions append immutable ledger rows for manual debugging.

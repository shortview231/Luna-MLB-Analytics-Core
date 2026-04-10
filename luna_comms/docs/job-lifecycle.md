# Job Lifecycle

`detected -> prepared -> queued -> sent -> acknowledged -> archived`

Failure branch:

`queued -> failed -> queued` (retry) and `failed -> dead_letter` (max attempts exceeded).

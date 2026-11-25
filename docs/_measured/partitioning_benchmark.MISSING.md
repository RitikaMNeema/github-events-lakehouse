# Partition layout benchmark — NOT YET VALID

This report is deliberately absent rather than wrong.

`make benchmark` was run and produced numbers, but the host was swapping
heavily at the time — 6.4 GB of 7.2 GB swap in use, ~16 MB free physical
memory — and the results were incoherent: the day-partitioned table
reported a *slower* full-table aggregate (0.682s) than the unpartitioned
one (0.147s), which is not physically possible. Under that much memory
pressure the measurement describes the swap subsystem, not the table layout.

Correctness results survive resource starvation. Performance results do not.

## To generate a valid report

1. Give Docker Desktop more memory (Settings → Resources). The machine has
   16 GiB physical and Docker was limited to 7.65 GiB.
2. Close memory-heavy applications, and stop unrelated containers.
3. Confirm the host is not swapping:

   ```
   sysctl -n vm.swapusage     # "used" should be a small fraction of total
   ```

4. `make benchmark` — takes about 20 seconds — then delete this file.

The script itself also now refuses to overstate its case: if the loaded
slice spans a single day, the generated report states plainly that
partition pruning is **not** being exercised, because a day-partitioned
table built from one day has exactly one partition and a single-day
predicate prunes nothing.

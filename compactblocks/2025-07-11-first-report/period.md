These nodes have already finished IBD, but I arbitrarily start measuring ~2
hours after the nodes have started, ~two minutes before they both reconstruct
block 000000000000000000022b052e4cfeef90d4b31a8e6596e094cfd259eb824488 without
any prefills, so at this point I consider them reasonably synchronized with the
network.

2025-06-21T03:00:00.000000Z

End time is selected as ~45 minutes before the prefilling node crashed after
running out of disk space.

2025-07-11T03:00:00.000000Z

So I ran the following commands:

```bash
export START_TIME="2025-06-21T03:00:00.000000Z"
export END_TIME="2025-07-11T03:00:00.000000Z"

./logfilters.sh manualcompactonly.log $START_TIME $END_TIME > filtered/manualcompactonly.log

./logfilters.sh prefiller.log $START_TIME $END_TIME > filtered/prefiller.log
```

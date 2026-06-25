# Required README Answers

## 1. What does the delta between `processed_at` and `confirmed_at` tell you?

It measures how long it took the cluster to move from "I observed this
transaction in a processed slot" to "enough stake has voted on that slot for
confirmed commitment."

In the mainnet campaign, several transactions confirmed almost immediately after
the stream saw them processed, while others took hundreds of milliseconds to
about 1.5 seconds. That delta is a compact network-health signal:

- small and stable means the leader produced the block and votes propagated
  quickly
- growing or erratic means the network is seeing vote propagation delay, fork
  churn, congestion, or leader/validator instability

It is not the same as transaction execution time. Execution already happened by
`processed_at`. The delta mostly tells you about commitment propagation and fork
confidence after the transaction first appears.

## 2. Why should you never fetch a blockhash at finalized commitment for a time-sensitive transaction?

A finalized blockhash is already old by the time it is finalized. Solana
blockhashes have a limited validity window, so fetching at `finalized`
unnecessarily burns a large part of that window before the transaction is even
signed and sent.

For a bundle or fast-path landing attempt, that is dangerous because the bundle
may wait for a Jito leader, hit transport delay, or need one agent-driven retry.
Starting with a stale finalized hash makes `BlockhashNotFound` or expiry much
more likely.

TxRadar fetches blockhashes at `confirmed`, tracks `lastValidBlockHeight`, and
refreshes before retrying. The campaign failures classified as
`expired_blockhash` are exactly why this matters: once the transaction misses
its validity window, the right recovery is a fresh blockhash and a new signed
transaction.

## 3. What happens to a bundle if the Jito leader skips their slot?

The bundle does not land in that skipped slot. Jito bundles are only useful when
a Jito-connected leader can actually produce the block that includes them. If
the target leader skips, the bundle is not included, the tip inside the bundle is
not paid, and the client must resubmit against a fresh leader opportunity while
the blockhash is still valid.

That is why TxRadar keeps the tip transfer inside the same transaction as the
memo instruction: failed or skipped inclusion does not leak the tip. The agent
then sees the non-landing or expiry classification and can decide whether to
refresh the blockhash, raise the tip, and resubmit.

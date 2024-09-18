import type { DbQueueMetrics, Queue, QueueMetrics } from "./types.ts";

/*

PGMQ internals:

select pgmq.list_queues();
                   list_queues
--------------------------------------------------
 (87t96jquy2,f,t,"2024-09-17 15:35:16.007417+00")
(1 row)

postgres=> \d pgmq.meta
                             Table "pgmq.meta"
     Column     |           Type           | Collation | Nullable | Default
----------------+--------------------------+-----------+----------+---------
 queue_name     | character varying        |           | not null |
 is_partitioned | boolean                  |           | not null |
 is_unlogged    | boolean                  |           | not null |
 created_at     | timestamp with time zone |           | not null | now()
Indexes:
    "meta_queue_name_key" UNIQUE CONSTRAINT, btree (queue_name)
    */

/*
    -- list queues
CREATE FUNCTION pgmq."list_queues"()
RETURNS SETOF pgmq.queue_record AS $$
BEGIN
  RETURN QUERY SELECT * FROM pgmq.meta;
END
$$ LANGUAGE plpgsql;

CREATE TYPE pgmq.queue_record AS (
    queue_name VARCHAR,
    is_partitioned BOOLEAN,
    is_unlogged BOOLEAN,
    created_at TIMESTAMP WITH TIME ZONE
);
*/

export const parseDbQueue = (q: string): Queue => {
  // Ensure the input has the expected number of parts (4)
  const parts = q.substring(1, q.length - 1).split(",");

  if (parts.length !== 4) {
    throw new Error("Malformed input: expected 4 parts");
  }

  return {
    name: parts[0],
    createdAt: new Date(parts[3]),
    isPartitioned: parts[1] === "t",
    isUnlogged: parts[2] === "t",
  };
};

export const parseDbQueueMetrics = (m: DbQueueMetrics): QueueMetrics => ({
  queueName: m.queue_name,
  queueLength: Number.parseInt(m.queue_length),
  newestMsgAgeSec: m.newest_msg_age_sec != null
    ? Number.parseInt(m.newest_msg_age_sec)
    : undefined,
  oldestMsgAgeSec: m.oldest_msg_age_sec != null
    ? Number.parseInt(m.oldest_msg_age_sec)
    : undefined,
  totalMessages: Number.parseInt(m.total_messages),
  scrapeTime: new Date(m.scrape_time),
});

import type { Pool } from "https://deno.land/x/postgres@v0.19.3/mod.ts";
import { parseDbQueue, parseDbQueueMetrics } from "./helpers.ts";
import type { DbQueueMetrics, Queue } from "./types.ts";

export class QueueManager {
  constructor(private readonly pool: Pool) {}

  // List all queues
  public async list(): Promise<Queue[]> {
    using client = await this.pool.connect();
    const result = await client.queryObject<{ list_queues: string }>(
      "SELECT pgmq.list_queues()",
    );
    return result.rows.map(({ list_queues }) => parseDbQueue(list_queues));
  }

  // Create a new queue
  public async create(name: string) {
    using client = await this.pool.connect();
    await client.queryObject("SELECT pgmq.create($1)", [name]); // Wrapping argument in an array
  }

  // Create an unlogged queue
  public async createUnlogged(name: string) {
    using client = await this.pool.connect();
    await client.queryObject("SELECT pgmq.create_unlogged($1)", [name]); // Wrapping argument in an array
  }

  public async createPartitioned(
    name: string,
    partitionInterval: string,
    retentionInterval: string,
  ) {
    using client = await this.pool.connect();
    await client.queryObject("SELECT pgmq.create_partitioned($1, $2, $3)", [
      name,
      partitionInterval,
      retentionInterval,
    ]);
  }

  // Drop an existing queue
  public async drop(name: string) {
    using client = await this.pool.connect();
    await client.queryObject("SELECT pgmq.drop_queue($1)", [name]); // Wrapping argument in an array
  }

  // Purge a queue
  public async purge(name: string) {
    using client = await this.pool.connect();
    await client.queryObject("SELECT pgmq.purge_queue($1)", [name]); // Wrapping argument in an array
  }

  // Detach the archive of a queue (FIXME: appears to be broken)
  public async detachArchive(name: string) {
    using client = await this.pool.connect();
    await client.queryObject("SELECT pgmq.detach_archive($1)", [name]); // Wrapping argument in an array
  }

  // Get metrics for a specific queue
  public async getMetrics(name: string) {
    using client = await this.pool.connect();
    const result = await client.queryObject<DbQueueMetrics>(
      "SELECT * FROM pgmq.metrics($1)",
      [name],
    ); // Wrapping argument in an array
    return parseDbQueueMetrics(result.rows[0]);
  }

  // Get metrics for all queues
  public async getAllMetrics() {
    using client = await this.pool.connect();
    const result = await client.queryObject<DbQueueMetrics>(
      "SELECT * FROM pgmq.metrics_all()",
    );
    return result.rows.map(parseDbQueueMetrics);
  }
}

import { Pool } from "https://deno.land/x/postgres@v0.19.3/mod.ts";
// Automatically load environment variables from a `.env` file
import "jsr:@std/dotenv/load";

import { QueueManager } from "./queue-manager/index.ts";
import { MsgManager } from "./msg-manager/index.ts";

// Use the connection string directly from environment variables
const defaultConnectionString = "postgres://user:password@localhost:5432/test";

// Function to create a connection pool
const createPool = (dsn?: string, maxPoolSize?: number, lazy?: boolean) => {
  const connectionString = dsn || Deno.env.get("DATABASE_URL") ||
    defaultConnectionString;
  const poolSize = maxPoolSize || Number(Deno.env.get("MAX_POOL_SIZE")) || 20; // Get max pool size from env or use provided value
  const isLazy = lazy || Deno.env.get("LAZY") === "true" || false; // Get lazy flag from env or use provided value
  return new Pool(connectionString, poolSize, isLazy); // Adjust pool size as needed
};

export class Pgmq {
  public readonly queue: QueueManager;
  public readonly msg: MsgManager;

  private constructor(private readonly pool: Pool) {
    this.queue = new QueueManager(pool);
    this.msg = new MsgManager(pool);
  }

  // Static method to create a new instance with config and defaults
  public static new(config?: {
    dsn?: string;
    lazy?: boolean;
    maxPoolSize?: number;
  }) {
    const pool = createPool(config?.dsn, config?.maxPoolSize, config?.lazy);
    const pgmq = new Pgmq(pool);
    return pgmq;
  }

  // Closing the pool when needed
  public async close() {
    await this.pool.end();
  }
}

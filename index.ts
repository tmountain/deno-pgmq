import { Pool } from "https://deno.land/x/postgres@v0.19.3/mod.ts";
// Automatically load environment variables from a `.env` file
import "jsr:@std/dotenv/load";

import { QueueManager } from "./queue-manager/index.ts";
import { MsgManager } from "./msg-manager/index.ts";
import { parseOptionsFromUri } from "./utils/uri.ts";

// Use the connection string directly from environment variables
const defaultConnectionString = "postgres://user:password@localhost:5432/test";

// Function to create a connection pool with optional CA files
const createPool = async (
	dsn?: string,
	maxPoolSize?: number,
	lazy?: boolean,
	caFilePaths?: string[],
) => {
	const connectionString =
		dsn || Deno.env.get("DATABASE_URL") || defaultConnectionString;
	const poolSize = maxPoolSize || Number(Deno.env.get("MAX_POOL_SIZE")) || 20;
	const isLazy = lazy || Deno.env.get("LAZY") === "true" || false;

	const options = await parseOptionsFromUri(connectionString, caFilePaths);

	return new Pool(options, poolSize, isLazy);
};

export class Pgmq {
	public readonly queue: QueueManager;
	public readonly msg: MsgManager;

	private constructor(private readonly pool: Pool) {
		this.queue = new QueueManager(pool);
		this.msg = new MsgManager(pool);
	}

	// Static method to create a new instance with config and CA files
	public static async new(config?: {
		dsn?: string;
		lazy?: boolean;
		maxPoolSize?: number;
		caFilePaths?: string[];
	}) {
		const pool = await createPool(
			config?.dsn,
			config?.maxPoolSize,
			config?.lazy,
			config?.caFilePaths,
		);
		const pgmq = new Pgmq(pool);
		return pgmq;
	}

	public async close() {
		await this.pool.end();
	}
}

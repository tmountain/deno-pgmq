import type { Pool } from "https://deno.land/x/postgres@v0.19.3/mod.ts";
import { parseDbMessage } from "./helpers.ts";
import type { DbMessage, Message } from "./types.ts";

export class MsgManager {
	constructor(private readonly pool: Pool) {}

	// Send a message to the queue
	public async send<T>(q: string, msg: T, delay = 0): Promise<number> {
		using client = await this.pool.connect();
		const res = await client.queryObject<{ send: number }>(
			"SELECT * FROM pgmq.send($1, $2, $3)",
			[q, JSON.stringify(msg), delay],
		);
		return res.rows[0].send;
	}

	// Send a batch of messages to the queue
	public async sendBatch<T>(
		q: string,
		msgs: T[],
		delay = 0,
	): Promise<number[]> {
		using client = await this.pool.connect();
		const res = await client.queryObject<{ send_batch: number }>(
			"SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3)",
			[q, msgs.map((m) => JSON.stringify(m)), delay],
		);
		return res.rows.flatMap((s) => s.send_batch);
	}

	// Read a single message from the queue
	public read<T>(q: string, vt = 0): Promise<Message<T>> {
		return this.readBatch<T>(q, vt, 1).then((msgs) => msgs[0]);
	}

	// Read a batch of messages from the queue
	public async readBatch<T>(
		q: string,
		vt: number,
		numMessages: number,
	): Promise<Message<T>[]> {
		using client = await this.pool.connect();
		const res = await client.queryObject<DbMessage>(
			"SELECT * FROM pgmq.read($1, $2, $3)",
			[q, vt, numMessages],
		);
		return res.rows.flatMap(parseDbMessage<T>) as Message<T>[];
	}

	// Pop a single message from the queue
	public async pop<T>(q: string): Promise<Message<T> | undefined> {
		using client = await this.pool.connect();
		const res = await client.queryObject<DbMessage>(
			"SELECT * FROM pgmq.pop($1)",
			[q],
		);
		return parseDbMessage<T>(res.rows[0]);
	}

	// Archive a single message
	public async archive(q: string, msgId: number): Promise<boolean> {
		using client = await this.pool.connect();
		const res = await client.queryObject<{ archive: boolean }>(
			"SELECT pgmq.archive($1, $2::bigint)",
			[q, msgId],
		);
		return res.rows[0].archive;
	}

	// Archive a batch of messages
	public async archiveBatch(q: string, msgIds: number[]): Promise<number[]> {
		using client = await this.pool.connect();
		const res = await client.queryObject<{ archive: number }>(
			"SELECT pgmq.archive($1, $2::bigint[])",
			[q, msgIds],
		);
		return res.rows.flatMap((a) => a.archive);
	}

	// Delete a single message
	public async delete(q: string, msgId: number): Promise<boolean> {
		using client = await this.pool.connect();
		const res = await client.queryObject<{ delete: boolean }>(
			"SELECT pgmq.delete($1, $2::bigint)",
			[q, msgId],
		);
		return res.rows[0].delete;
	}

	// Delete a batch of messages
	public async deleteBatch(q: string, msgIds: number[]): Promise<number[]> {
		using client = await this.pool.connect();
		const res = await client.queryObject<{ delete: number }>(
			"SELECT pgmq.delete($1, $2::bigint[])",
			[q, msgIds],
		);
		return res.rows.flatMap((d) => d.delete);
	}

	// Set the visibility timeout of a message
	public async setVt<T>(
		q: string,
		msgId: number,
		vtOffset: number,
	): Promise<Message<T> | undefined> {
		using client = await this.pool.connect();
		const res = await client.queryObject<DbMessage>(
			"SELECT * FROM pgmq.set_vt($1, $2, $3);",
			[q, msgId, vtOffset],
		);
		return parseDbMessage<T>(res.rows[0]);
	}
}

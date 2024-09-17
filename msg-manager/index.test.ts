import { assert, assertStrictEquals, assertRejects } from "jsr:@std/assert";
import { Pgmq } from "../index.ts";
import type { Message } from "./types.ts";
import { faker } from "https://deno.land/x/deno_faker@v1.0.3/mod.ts";

type TestMessage = { data: string };

// Test with real database
Deno.test("MsgManager integration test with real DB", async (t) => {
	// Initialize the Pgmq instance with a real DB connection
	const pgmq = Pgmq.new();

	// Define the test queue name
	const testQueue = "test_queue";

	await t.step("Clean up queue", async () => {
		await pgmq.queue.drop(testQueue);
	});

	await t.step("Set up queue", async () => {
		await pgmq.queue.create(testQueue);
	});

	await t.step("Test send single message", async () => {
		const msgId = await pgmq.msg.send(testQueue, { data: "test message" }, 0);
		assert(msgId > 0, "Message ID should be greater than 0");
	});

	// Helper function for deleting messages
	async function deleteMessage(pgmq: Pgmq, queue: string, msgId: number) {
		const deleteResult = await pgmq.msg.delete(queue, msgId);
		assert(deleteResult, "Message should be deleted");
	}

	// Updated tests using delete helper

	await t.step("Test read single message", async () => {
		const message = await pgmq.msg.read<TestMessage>(testQueue, 0);

		assertStrictEquals(
			message.message.data,
			"test message",
			"Message content should match",
		);

		// Use delete helper to delete the message after reading
		await deleteMessage(pgmq, testQueue, message.msgId);
	});

	await t.step("Test send batch messages", async () => {
		const msgIds = await pgmq.msg.sendBatch(
			testQueue,
			[{ data: "test batch message 1" }, { data: "test batch message 2" }],
			0,
		);
		assertStrictEquals(msgIds.length, 2, "Two messages should be sent");
	});

	await t.step("Test read batch messages", async () => {
		// `readBatch` also parses the messages via `parseDbMessage`
		const messages: Message<TestMessage>[] = await pgmq.msg.readBatch(
			testQueue,
			0,
			2,
		);

		assertStrictEquals(messages.length, 2, "Should return 2 messages");
		assertStrictEquals(
			messages[0].message.data,
			"test batch message 1",
			"First message content should match",
		);
		assertStrictEquals(
			messages[1].message.data,
			"test batch message 2",
			"Second message content should match",
		);
	});

	await t.step("Test archive single message", async () => {
		const message: Message<TestMessage> = await pgmq.msg.read<TestMessage>(
			testQueue,
			0,
		);

		const archiveResult = await pgmq.msg.archive(testQueue, message.msgId);
		assert(archiveResult, "Message should be archived");
	});

	await t.step("Test delete single message", async () => {
		const message: Message<TestMessage> = await pgmq.msg.read<TestMessage>(
			testQueue,
			0,
		);

		const deleteResult = await pgmq.msg.delete(testQueue, message.msgId);
		assert(deleteResult, "Message should be deleted");
	});

	await t.step("Clean up queue", async () => {
		await pgmq.queue.drop(testQueue);
	});

	await t.step("Set up queue", async () => {
		await pgmq.queue.create(testQueue);
	});

	await t.step("Test delete message so it can't be read again", async () => {
		const testMessage = { data: "msg" };

		// Send a message to the queue
		await pgmq.msg.send(testQueue, testMessage);

		// Pop the message from the queue
		const poppedMessage = await pgmq.msg.pop(testQueue);
		assert(poppedMessage !== undefined, "Message should be popped");

		// Ensure the queue is empty after popping
		const emptyQueueCheck = await pgmq.msg.pop(testQueue);
		assertStrictEquals(
			emptyQueueCheck,
			undefined,
			"Queue should be empty after popping the message",
		);
	});

	await t.step("Test queue is empty, returns undefined", async () => {
		// Pop from an empty queue
		const poppedMessage = await pgmq.msg.pop(testQueue);
		assertStrictEquals(
			poppedMessage,
			undefined,
			"Popped message should be undefined since queue is empty",
		);
	});

	await t.step("Clean up queue", async () => {
		await pgmq.queue.drop(testQueue);
	});

	await t.step("Set up queue", async () => {
		await pgmq.queue.create(testQueue);
	});

	await t.step("Test returns true if message is archived", async () => {
		// Send a message to the queue
		const msgId = await pgmq.msg.send(testQueue, { data: "msg" });

		// Archive the message
		const archived = await pgmq.msg.archive(testQueue, msgId);
		assertStrictEquals(archived, true, "Message should be archived");
	});

	await t.step(
		"Test archives the message so it can't be read again",
		async () => {
			// Send a message to the queue
			const msgId = await pgmq.msg.send(testQueue, { data: "msg" });

			// Archive the message
			await pgmq.msg.archive(testQueue, msgId);

			// Ensure the queue is empty after archiving
			const emptyQueueCheck = await pgmq.msg.pop(testQueue);
			assertStrictEquals(
				emptyQueueCheck,
				undefined,
				"Queue should be empty after archiving the message",
			);
		},
	);

	await t.step("Test returns false if no such message id", async () => {
		// Attempt to archive a non-existent message
		const archived = await pgmq.msg.archive(testQueue, 1);
		assertStrictEquals(
			archived,
			false,
			"Archiving a non-existent message should return false",
		);
	});

	await t.step("Clean up queue", async () => {
		await pgmq.queue.drop(testQueue);
	});

	await t.step("Set up queue", async () => {
		await pgmq.queue.create(testQueue);
	});

	await t.step("Test returns true if message is archived", async () => {
		// Send a message to the queue
		const msgId = await pgmq.msg.send(testQueue, { data: "msg" });

		// Archive the message
		const archived = await pgmq.msg.archive(testQueue, msgId);
		assertStrictEquals(archived, true, "Message should be archived");
	});

	await t.step(
		"Test archives the message so it can't be read again",
		async () => {
			// Send a message to the queue
			const msgId = await pgmq.msg.send(testQueue, { data: "msg" });

			// Archive the message
			await pgmq.msg.archive(testQueue, msgId);

			// Ensure the queue is empty after archiving
			const emptyQueueCheck = await pgmq.msg.pop(testQueue);
			assertStrictEquals(
				emptyQueueCheck,
				undefined,
				"Queue should be empty after archiving the message",
			);
		},
	);

	await t.step("Test returns false if no such message id", async () => {
		// Attempt to archive a non-existent message
		const archived = await pgmq.msg.archive(testQueue, 1);
		assertStrictEquals(
			archived,
			false,
			"Archiving a non-existent message should return false",
		);
	});

	await t.step("Clean up queue", async () => {
		await pgmq.queue.drop(testQueue);
	});

	await t.step("Set up queue", async () => {
		await pgmq.queue.create(testQueue);
	});

	await t.step("Test returns true if message is deleted", async () => {
		const msg = { data: "msg" };
		const msgId = await pgmq.msg.send(testQueue, msg);

		const deleted = await pgmq.msg.delete(testQueue, msgId);
		assertStrictEquals(deleted, true, "Message should be deleted");
	});

	await t.step(
		"Test deletes the message so it can't be read again",
		async () => {
			const msg = { data: "msg" };
			const msgId = await pgmq.msg.send(testQueue, msg);

			const deleted = await pgmq.msg.delete(testQueue, msgId);
			assertStrictEquals(deleted, true, "Message should be deleted");

			const read = await pgmq.msg.read<string>(testQueue);
			assertStrictEquals(
				read,
				undefined,
				"Message should no longer be readable after deletion",
			);
		},
	);

	await t.step("Test returns false if no such message id", async () => {
		const deleted = await pgmq.msg.delete(testQueue, 1);
		assertStrictEquals(
			deleted,
			false,
			"Deleting a non-existent message should return false",
		);
	});

	await t.step("Clean up queue", async () => {
		await pgmq.queue.drop(testQueue);
	});

	await t.step("Set up queue", async () => {
		await pgmq.queue.create(testQueue);
	});

	await t.step("Test returns the message ids that were deleted", async () => {
		const msg1 = { data: "msg1" };
		const msg2 = { data: "msg2" };
		const [id1, id2] = await pgmq.msg.sendBatch(testQueue, [msg1, msg2]);

		const [dId1, dId2] = await pgmq.msg.deleteBatch(testQueue, [id1, id2]);

		assertStrictEquals(dId1, id1, "First deleted message ID should match");
		assertStrictEquals(dId2, id2, "Second deleted message ID should match");
	});

	await t.step(
		"Test deletes the messages so they can't be read again",
		async () => {
			const msg1 = { data: "msg1" };
			const msg2 = { data: "msg2" };
			const [id1, id2] = await pgmq.msg.sendBatch(testQueue, [msg1, msg2]);

			await pgmq.msg.deleteBatch(testQueue, [id1, id2]);

			const noMsg = await pgmq.msg.read<string>(testQueue);
			assertStrictEquals(
				noMsg,
				undefined,
				"Messages should no longer be readable after deletion",
			);
		},
	);

	await t.step("Test does not return non-existing message ids", async () => {
		const msg = { data: "msg" };
		const id = await pgmq.msg.send(testQueue, msg);

		const deleted = await pgmq.msg.deleteBatch(testQueue, [id, 2, 3, 4, 5]);

		assertStrictEquals(
			deleted.length,
			1,
			"Only existing message ID should be returned",
		);
		assertStrictEquals(
			deleted[0],
			id,
			"Returned ID should match the sent message ID",
		);
	});

	await t.step(
		"Test returns an empty array if no such message ids",
		async () => {
			const deleted = await pgmq.msg.deleteBatch(testQueue, [1, 2, 3, 4, 5]);
			assertStrictEquals(
				deleted.length,
				0,
				"No message IDs should be returned for non-existent messages",
			);
		},
	);

	await t.step("Clean up queue", async () => {
		await pgmq.queue.drop(testQueue);
	});

	await t.step("Set up queue", async () => {
		await pgmq.queue.create(testQueue);
	});

	await t.step("Test sets the vt", async () => {
		const msgId = await pgmq.msg.send(testQueue, { data: "msg" });
		const msg = await pgmq.msg.setVt(testQueue, msgId, 30);

		assert(msg !== undefined, "Message should be defined");
		assert(
			Math.abs(msg?.vt.valueOf() - (new Date().valueOf() + 30 * 1000)) < 1000,
			"VT should be set correctly",
		);
	});

	await t.step(
		"Test does not increment vt when called twice but resets it",
		async () => {
			const msgId = await pgmq.msg.send(testQueue, { data: "msg" });
			const msg = await pgmq.msg.setVt(testQueue, msgId, 10);

			assert(msg !== undefined, "Message should be defined");
			assert(
				Math.abs(msg?.vt.valueOf() - (new Date().valueOf() + 10 * 1000)) < 1000,
				"VT should be set to 10 seconds",
			);
		},
	);

	await t.step("Test returns the message", async () => {
		const msgId = await pgmq.msg.send(testQueue, { data: "msg" });
		const msg = await pgmq.msg.setVt<{ data: string }>(testQueue, msgId, 30);

		assert(msg !== undefined, "Message should be defined");
		assertStrictEquals(
			msg?.message.data,
			"msg",
			"Message content should match",
		);
	});

	await t.step("Test returns undefined when no such msgId", async () => {
		const msg = await pgmq.msg.setVt(testQueue, 10000, 30);
		assertStrictEquals(
			msg,
			undefined,
			"Message should be undefined for non-existent msgId",
		);
	});

	await t.step("Test rejects when no such queue", async () => {
		await assertRejects(
			async () => {
				await pgmq.msg.setVt("non_existing_queue", 10000, 30);
			},
			Error,
			'relation "pgmq.q_non_existing_queue" does not exist', // Expect the actual error message
		);
	});

	await t.step("Clean up queue", async () => {
		await pgmq.queue.drop(testQueue);
	});

	await t.step("Set up queue", async () => {
		await pgmq.queue.create(testQueue);
	});

	const inputMsgs = [
		{ msg: faker.random.number(), type: "int" },
		{ msg: faker.random.float(), type: "float" },
		{ msg: faker.random.alphaNumeric(5), type: "string" },
		{ msg: faker.random.boolean(), type: "boolean" },
		{ msg: faker.random.alphaNumeric(5).split(""), type: "array" },
		{
			msg: {
				a: faker.random.number(),
				b: faker.random.boolean(),
				c: faker.random.alphaNumeric(5),
			},
			type: "object",
		},
		{ msg: new Date(), type: "date" },
		{ msg: null, type: "null" },
		{ msg: undefined, type: "undefined" },
	];

	await t.step("Test accepts different message types", async () => {
		for (const { msg } of inputMsgs) {
			await pgmq.msg.send(testQueue, msg);
		}
	});

	await t.step("Test returns unique message id", async () => {
		const id = await pgmq.msg.send(testQueue, { data: "msg" });

		// Use deleteMessage helper to clean up after test
		await deleteMessage(pgmq, testQueue, id);
	});

	await t.step("Clean up queue", async () => {
		await pgmq.queue.drop(testQueue);
	});

	await t.step("Set up queue", async () => {
		await pgmq.queue.create(testQueue);
	});

	await t.step(
		'Test does not read a read message within the "vt" window',
		async () => {
			const delay = (ms: number) =>
				new Promise((resolve) => setTimeout(resolve, ms));

			interface T {
				id: number;
				msg: string;
				date?: Date;
				isGood: boolean;
			}

			const msg = { id: 1, msg: "msg", isGood: true };
			await pgmq.msg.send(testQueue, msg);
			const vt = 1; // 1 second visibility timeout

			// First read, should succeed
			const readMsg = await pgmq.msg.read<T>(testQueue, vt);
			assertStrictEquals(
				JSON.stringify(readMsg?.message),
				JSON.stringify(msg),
				"First message should be read",
			);

			// Immediately try to read again, should return undefined
			const noMsg = await pgmq.msg.read<T>(testQueue, vt);
			assertStrictEquals(
				noMsg,
				undefined,
				"Message should not be read again within the VT window",
			);

			// Wait for the VT to expire
			await delay((vt + 1) * 1000); // Adding a buffer to ensure VT has expired

			// After the VT expires, we should be able to read the message again
			const readMsg2 = await pgmq.msg.read<T>(testQueue, vt);
			assertStrictEquals(
				JSON.stringify(readMsg2?.message),
				JSON.stringify(msg),
				"Message should be readable again after VT expires",
			);
		},
	);

	await t.step('Test rejects floating points in the "vt" window', async () => {
		interface T {
			id: number;
			msg: string;
			date?: Date;
			isGood: boolean;
		}

		const msg = { id: 1, msg: "msg", isGood: true };
		await pgmq.msg.send(testQueue, msg);

		await assertRejects(
			async () => {
				await pgmq.msg.read<T>(testQueue, 1.5);
			},
			Error,
			'invalid input syntax for type integer: "1.5"', // Expecting the actual PostgreSQL error message
		);
	});

	// Clean up and close the pool after the test suite
	await pgmq.close();
});

import { assertArrayIncludes, assertStrictEquals } from "jsr:@std/assert";
import { expect } from "jsr:@std/expect";
import { faker } from "https://deno.land/x/deno_faker@v1.0.3/mod.ts";
import { Pgmq } from "../index.ts"; // Assuming Pgmq is being imported from the module you're working on

const pgmq = await Pgmq.new({ lazy: true }); // Assuming you have a similar setup for pgmq instance

// Helper function to get a queue by name
const getQueue = async (name: string) => {
  const queues = await pgmq.queue.list();
  return queues.find((q) => q.name === name);
};

Deno.test("Queue Manager Tests", async (t) => {
  await t.step("Clean up queues", async () => {
    const queues = await pgmq.queue.list();
    await Promise.all(queues.map((q) => pgmq.queue.drop(q.name)));
  });

  await t.step("Test returns an empty list; no queues", async () => {
    const queues = await pgmq.queue.list();
    assertStrictEquals(queues.length, 0, "Queue list should be empty");
  });

  await t.step("Test returns a list of queues; one queue", async () => {
    const qName = faker.random.alphaNumeric(10);
    await pgmq.queue.create(qName);

    const queues = await pgmq.queue.list();
    const queue = await getQueue(qName);
    assertArrayIncludes(
      queues,
      [queue],
      "Queue list should contain the created queue",
    );

    // Cleanup: Drop the created queue
    await pgmq.queue.drop(qName);
  });

  await t.step("Test returns a list of queues; multiple queues", async () => {
    const qName1 = faker.random.alphaNumeric(10);
    const qName2 = faker.random.alphaNumeric(10);
    const qName3 = faker.random.alphaNumeric(10);

    // Create multiple queues
    await Promise.all([
      pgmq.queue.create(qName1),
      pgmq.queue.create(qName2),
      pgmq.queue.create(qName3),
    ]);

    const queues = await pgmq.queue.list();

    // Extract only the fields we're interested in for comparison
    const filteredQueues = queues.map((q) => ({
      name: q.name,
      isPartitioned: q.isPartitioned,
      isUnlogged: q.isUnlogged,
    }));

    assertArrayIncludes(
      filteredQueues,
      [
        { name: qName1, isPartitioned: false, isUnlogged: false },
        { name: qName2, isPartitioned: false, isUnlogged: false },
        { name: qName3, isPartitioned: false, isUnlogged: false },
      ],
      "Queue list should contain the created queues",
    );

    // Cleanup: Drop the created queues
    await Promise.all([qName1, qName2, qName3].map((q) => pgmq.queue.drop(q)));
  });

  await t.step("It creates a new queue", async () => {
    const qName = faker.random.alphaNumeric(10);
    await pgmq.queue.create(qName);
    const queue = await getQueue(qName);
    assertStrictEquals(queue?.name, qName, "Queue name should match");
    await pgmq.queue.drop(qName);
  });

  await t.step("It creates an unlogged queue", async () => {
    const qName = faker.random.alphaNumeric(10);
    await pgmq.queue.createUnlogged(qName);
    const queue = await getQueue(qName);
    assertStrictEquals(queue?.name, qName, "Queue name should match");
    assertStrictEquals(
      queue?.isUnlogged,
      true,
      `Queue ${qName} should be unlogged`,
    );
    await pgmq.queue.drop(qName);
  });

  await t.step(
    "It fails to create an unlogged queue with the same name as an existing logged queue",
    async () => {
      const qName = faker.random.alphaNumeric(10);
      await pgmq.queue.create(qName);
      expect(pgmq.queue.createUnlogged(qName)).rejects.toThrow();
      await pgmq.queue.drop(qName);
    },
  );

  await t.step("It drops a queue", async () => {
    const qName = faker.random.alphaNumeric(10);
    await pgmq.queue.create(qName);
    await pgmq.queue.drop(qName);
    const queue = await getQueue(qName);
    assertStrictEquals(queue, undefined, "Queue should not exist");
  });

  await t.step("It purges a queue", async () => {
    const qName = faker.random.alphaNumeric(10);
    await pgmq.queue.create(qName);
    await pgmq.msg.send(qName, { text: "Hello, world!" });
    await pgmq.queue.purge(qName);
    const metrics = await pgmq.queue.getMetrics(qName);
    assertStrictEquals(metrics.queueLength, 0, "Queue should be empty");
    await pgmq.queue.drop(qName);
  });

  /*
	await t.step("It detaches the archive of a queue", async () => {
		const qName = faker.random.alphaNumeric(10);
		await pgmq.queue.create(qName);
		await pgmq.queue.detachArchive(qName);
		const queue = await getQueue(qName);
		assertStrictEquals(queue?.name, qName, "Queue name should match");
		await pgmq.queue.drop(qName);
	});
    */

  await t.step("It gets metrics for a specific queue", async () => {
    const qName = faker.random.alphaNumeric(10);
    await pgmq.queue.create(qName);
    const metrics = await pgmq.queue.getMetrics(qName);
    assertStrictEquals(metrics.queueName, qName, "Queue name should match");
    await pgmq.queue.drop(qName);
  });

  await t.step("It gets metrics for all queues", async () => {
    const qName1 = faker.random.alphaNumeric(10);
    const qName2 = faker.random.alphaNumeric(10);
    const qName3 = faker.random.alphaNumeric(10);

    // Create multiple queues
    await Promise.all([
      pgmq.queue.create(qName1),
      pgmq.queue.create(qName2),
      pgmq.queue.create(qName3),
    ]);

    const metrics = await pgmq.queue.getAllMetrics();
    const queueNames = metrics.map((m) => m.queueName);
    assertArrayIncludes(
      queueNames,
      [qName1, qName2, qName3],
      "Queue names should match",
    );

    // Cleanup: Drop the created queues
    await Promise.all([qName1, qName2, qName3].map((q) => pgmq.queue.drop(q)));
  });

  await t.step("Clean up queues", async () => {
    const queues = await pgmq.queue.list();
    await Promise.all(queues.map((q) => pgmq.queue.drop(q.name)));
  });

  await t.step(
    "It returns empty metrics for a non-existent queue",
    async () => {
      const metrics = await pgmq.queue.getAllMetrics();
      expect(metrics).toEqual([]);
    },
  );

  /*
     * fails becaues pg_partman is required for partitioned queues
	await t.step("It creates a partitioned queue", async () => {
		const qName = faker.random.alphaNumeric(10);
		const partitionInterval = "100000";
		const retentionInterval = "10000000";
		await pgmq.queue.createPartitioned(
			qName,
			partitionInterval,
			retentionInterval,
		);
		const queue = await getQueue(qName);
		assertStrictEquals(queue?.name, qName, "Queue name should match");
		assertStrictEquals(
			queue?.isPartitioned,
			true,
			"Queue should be partitioned",
		);
		await pgmq.queue.drop(qName);
	});
    */

  // Clean up and close the pool after the test suite
  await pgmq.close();
});

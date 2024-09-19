// Import the necessary Deno testing utilities
import { assert, assertEquals } from "jsr:@std/assert";
import { Pgmq } from "./index.ts";

// Test to ensure Pgmq can be created and closed successfully
Deno.test("Pgmq initialization and connection test", async () => {
  // Initialize Pgmq with the DATABASE_URL from the environment
  const pgmq = await Pgmq.new();

  // Verify that the instance is properly created
  assert(pgmq instanceof Pgmq, "Pgmq instance should be created");

  // You can add further checks or operations on pgmq.queue and pgmq.msg
  // For now, just verify that the queue manager and msg manager exist
  assert(pgmq.queue, "QueueManager should be initialized");
  assert(pgmq.msg, "MsgManager should be initialized");

  // Close the connection and verify no errors are thrown
  await pgmq.close();

  // Add a simple assertion to indicate that everything ran smoothly
  assertEquals(true, true, "Test completed successfully");
});

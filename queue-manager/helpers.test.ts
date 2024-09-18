import { assertEquals, assertThrows } from "jsr:@std/assert";
import { parseDbQueue } from "./helpers.ts";

Deno.test("parseDbQueue parses valid input correctly", () => {
  const input = '(87t96jquy2,f,t,"2024-09-17 15:35:16.007417+00")';
  const result = parseDbQueue(input);

  assertEquals(result.name, "87t96jquy2");
  assertEquals(result.isPartitioned, false);
  assertEquals(result.isUnlogged, true);
  assertEquals(
    result.createdAt.toISOString(),
    new Date("2024-09-17T15:35:16.007Z").toISOString(),
  );
});

Deno.test("parseDbQueue handles different boolean flags", () => {
  const input = '(abc123,t,f,"2023-12-10 10:30:00+00")';
  const result = parseDbQueue(input);

  assertEquals(result.name, "abc123");
  assertEquals(result.isPartitioned, true); // t -> true
  assertEquals(result.isUnlogged, false); // f -> false
});

Deno.test("parseDbQueue throws error on malformed input", () => {
  assertThrows(
    () => {
      parseDbQueue("(abc123,f)"); // Malformed input
    },
    Error,
    "Malformed input: expected 4 parts",
  ); // Update this to match the actual error message
});

Deno.test("parseDbQueue handles edge cases for dates", () => {
  const input = '(abc123,f,t,"2000-01-01 00:00:00+00")';
  const result = parseDbQueue(input);

  assertEquals(
    result.createdAt.toISOString(),
    new Date("2000-01-01T00:00:00.000Z").toISOString(),
  );
});

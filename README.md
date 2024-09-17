# deno-pgmq

Postgres Message Queue (PGMQ) Deno Client Library

## Installation

To use deno-pgmq in your Deno project, simply import it directly from the module URL. For example:


```bash
import { Pgmq } from "https://deno.land/x/your_module_name@version/mod.ts"; // Replace with your actual module URL and version
```

## Environment Variables (Examples)
```
DATABASE_URL=postgres://postgres:password@localhost:5432/postgres
MAX_POOL_SIZE=20
```

## Usage

First, Start a Postgres instance with the PGMQ extension installed:

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=password -p 5432:5432 quay.io/tembo/pgmq-pg:latest
```

Then:

```ts
// Import the Pgmq class from your module
import { Pgmq } from "your-database-module-path"; // Replace with your actual module path

// Load environment variables from a .env file if needed
import "@std/dotenv/load";

console.log("Connecting to Postgres...");
const pgmq = await Pgmq.new({
    dsn: Deno.env.get("DATABASE_URL"), // Use the DSN from the environment variable
    lazy: false, // Set lazy loading based on your preference
    maxPoolSize: Number(Deno.env.get("MAX_POOL_SIZE")) || 20, // Use max pool size from env
}).catch((err) => {
    console.error("Failed to connect to Postgres", err);
    Deno.exit(1);
});

const qName = "my_queue";
console.log(`Creating queue ${qName}...`);
await pgmq.queue.create(qName).catch((err) => {
    console.error("Failed to create queue", err);
    Deno.exit(1);
});

interface Msg {
    id: number;
    name: string;
}
const msg: Msg = { id: 1, name: "testMsg" };
console.log("Sending message...");
const msgId = await pgmq.msg.send(qName, msg).catch((err) => {
    console.error("Failed to send message", err);
    Deno.exit(1);
});

const vt = 30;
const receivedMsg = await pgmq.msg.read<Msg>(qName, vt).catch((err) => {
    console.error("No messages in the queue", err);
    Deno.exit(1);
});

console.log("Received message...");
if (receivedMsg) {
    console.dir(receivedMsg.message, { depth: null });
} else {
    console.log("No message received.");
}

console.log("Archiving message...");
await pgmq.msg.archive(qName, msgId).catch((err) => {
    console.error("Failed to archive message", err);
    Deno.exit(1);
});
```

## API

## Supported Functionalities

- [x] [Sending Messages](https://tembo-io.github.io/pgmq/api/sql/functions/#sending-messages)
  - [x] [send](https://tembo-io.github.io/pgmq/api/sql/functions/#send)
  - [x] [send_batch](https://tembo-io.github.io/pgmq/api/sql/functions/#send_batch)
- [ ] [Reading Messages](https://tembo-io.github.io/pgmq/api/sql/functions/#reading-messages)
  - [x] [read](https://tembo-io.github.io/pgmq/api/sql/functions/#read)
  - [ ] [read_with_poll](https://tembo-io.github.io/pgmq/api/sql/functions/#read_with_poll)
  - [x] [pop](https://tembo-io.github.io/pgmq/api/sql/functions/#pop)
- [x] [Deleting/Archiving Messages](https://tembo-io.github.io/pgmq/api/sql/functions/#deletingarchiving-messages)
  - [x] [delete (single)](https://tembo-io.github.io/pgmq/api/sql/functions/#delete-single)
  - [x] [delete (batch)](https://tembo-io.github.io/pgmq/api/sql/functions/#delete-batch)
  - [x] [purge_queue](https://tembo-io.github.io/pgmq/api/sql/functions/#purge_queue)
  - [x] [archive (single)](https://tembo-io.github.io/pgmq/api/sql/functions/#archive-single)
  - [x] [archive (batch)](https://tembo-io.github.io/pgmq/api/sql/functions/#archive-batch)
- [ ] [Queue Management](https://tembo-io.github.io/pgmq/api/sql/functions/#queue-management)
  - [x] [create](https://tembo-io.github.io/pgmq/api/sql/functions/#create)
  - [ ] [create_partitioned](https://tembo-io.github.io/pgmq/api/sql/functions/#create_partitioned)
  - [x] [create_unlogged](https://tembo-io.github.io/pgmq/api/sql/functions/#create_unlogged)
  - [x] [detach_archive](https://tembo-io.github.io/pgmq/api/sql/functions/#detach_archive)
  - [x] [drop_queue](https://tembo-io.github.io/pgmq/api/sql/functions/#drop_queue)
- [x] [Utilities](https://tembo-io.github.io/pgmq/api/sql/functions/#utilities)
  - [x] [set_vt](https://tembo-io.github.io/pgmq/api/sql/functions/#set_vt)
  - [x] [list_queues](https://tembo-io.github.io/pgmq/api/sql/functions/#list_queues)
  - [x] [metrics](https://tembo-io.github.io/pgmq/api/sql/functions/#metrics)
  - [x] [metrics_all](https://tembo-io.github.io/pgmq/api/sql/functions/#metrics_all)

## Testing

To run the tests, use the following command:

  ```
  deno test --allow-net --allow-env --allow-read
  ```
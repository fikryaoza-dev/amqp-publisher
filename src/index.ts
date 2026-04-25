// index.ts
import * as core from "@actions/core";
import * as amqp from "amqplib";
import * as dotenv from "dotenv";

dotenv.config();

// ─────────────────────────────────────────────
// Utils
// ─────────────────────────────────────────────
function isJSON(input: string): boolean {
  try {
    JSON.parse(input);
    return true;
  } catch {
    return false;
  }
}

function parsePayload(payload: string) {
  const json = isJSON(payload);
  return {
    data: json ? JSON.parse(payload) : payload,
    contentType: json ? "application/json" : "text/plain",
  };
}

function timeout(ms: number): Promise<never> {
  return new Promise((_, reject) =>
    setTimeout(() => reject(new Error("Connection timeout")), ms),
  );
}

// ─────────────────────────────────────────────
// RabbitMQ Publisher
// ─────────────────────────────────────────────
async function publishMessage(
  amqpUrl: string,
  exchange: string,
  queue: string,
  routingKey: string,
  message: string,
  contentType: string,
) {
  try {
    const connection = await amqp.connect(amqpUrl);
    const channel = await connection.createChannel();

    // 1. Exchange
    await channel.assertExchange(exchange, "direct", {
      durable: true,
    });

    // 2. Queue
    await channel.assertQueue(queue, {
      durable: true,
    });

    // 3. Binding (VERY IMPORTANT)
    await channel.bindQueue(queue, exchange, routingKey);

    // 4. Publish
    channel.publish(exchange, routingKey, Buffer.from(message), {
      contentType,
      contentEncoding: "utf-8",
      persistent: true,
      mandatory: true,
    });
  } catch (err) {
    console.error("❌ Connection failed:", err);
  } finally {
    console.error("❌ Connection closed:");
  }
}

// ─────────────────────────────────────────────
// Main Runner
// ─────────────────────────────────────────────
async function run() {
  try {
    const rawPayload = core.getInput("payload");

    if (!rawPayload) {
      throw new Error("Payload is required");
    }

    const { data, contentType } = parsePayload(rawPayload);

    // safer access
    const amqpUrl =
      typeof data === "object" ? data?.amqp_config?.rabbitmq_host : null;

    if (!amqpUrl) {
      throw new Error("RabbitMQ host not found in payload");
    }

    const exchange = process.env.EXCHANGE_NAME || "gitaction_exchange";
    const routingKey = process.env.ROUTING_KEY || "";
    const queue = process.env.QUEUE_NAME || "";
    if (!routingKey) {
      throw new Error("ROUTING_KEY is required");
    }
    console.log("📡 RabbitMQ Config:");
    console.log({
      exchange,
      routingKey,
      queue,
    });
    for (let i = 0; i < 3; i++) {
      try {
        await publishMessage(
          amqpUrl,
          exchange,
          queue,
          routingKey,
          rawPayload,
          contentType,
        );
        break;
      } catch (e) {
        if (i === 2) throw e;
      }
    }
  } catch (error) {
    if (error instanceof Error) {
      core.setFailed(`❌ ${error.message}`);
    }
  }
}

run();

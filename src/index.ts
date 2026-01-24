// index.ts
import * as core from "@actions/core"; // Note the '* as' for correct import
import * as amqp from "amqplib";
import * as dotenv from "dotenv";

function checkMsgIsJSON(msg: string) {
  try {
    JSON.parse(msg);
  } catch (error) {
    console.log("error parse :: %s", error);
    return false;
  }
  return true;
}

async function run() {
  try {
    dotenv.config();
    const PAYLOAD = core.getInput("payload");
    const msgIsJSON = checkMsgIsJSON(PAYLOAD);
    const payload = msgIsJSON ? JSON.parse(PAYLOAD) : PAYLOAD;
    const contentType = msgIsJSON ? "application/json" : undefined;
    if (contentType) {
      // Use @actions/core to get inputs or log messages
      const amqpUrl = payload.amqp_config.rabbitmq_host;
      core.info(`Connecting to RabbitMQ at  ${amqpUrl}...`);
      // Use amqplib to connect and interact with the broker
      const connection = (await Promise.race([
        amqp.connect(amqpUrl),
        timeout(10000), // 10 second limit
      ])) as amqp.Connection;

      const channel = await connection.createChannel();
      const queue = process.env.QUEUE_NAME || "github_action";
      const exchangeName = process.env.EXCHANGE_NAME || "gitaction_exchange";
      core.info(`Connected to RabbitMQ at  ${amqpUrl}...`);
      const routingKey = process.env.ROUTING_KEY || "";
      await channel.assertExchange(exchangeName, "direct", {
        durable: false, // Exchange is deleted when broker restarts
      });
      console.log("Assert Exchange :: ");
      await channel.assertQueue(queue, { durable: false });
      const options = {
        contentType,
        contentEncoding: "utf-8", // Recommended to set encoding as well
        persistent: true, // Example of another common option
      };
      console.log("Publish Message :: ");
      channel.publish(exchangeName, routingKey, Buffer.from(PAYLOAD), options);
      // channel.sendToQueue(queue, Buffer.from(message), { persistent: true });
      // await channel.bindQueue(queue, exchangeName, routingKey);
      core.info(`Sent message: "${PAYLOAD}"`);

      await channel.close();
      await connection.close();
    }
  } catch (error) {
    if (error instanceof Error) {
      core.setFailed(error.message);
    }
  }
}

run();

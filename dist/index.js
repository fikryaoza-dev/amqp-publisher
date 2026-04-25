"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
// index.ts
const core = __importStar(require("@actions/core"));
const amqp = __importStar(require("amqplib"));
const dotenv = __importStar(require("dotenv"));
dotenv.config();
// ─────────────────────────────────────────────
// Utils
// ─────────────────────────────────────────────
function isJSON(input) {
    try {
        JSON.parse(input);
        return true;
    }
    catch {
        return false;
    }
}
function parsePayload(payload) {
    const json = isJSON(payload);
    return {
        data: json ? JSON.parse(payload) : payload,
        contentType: json ? "application/json" : "text/plain",
    };
}
function timeout(ms) {
    return new Promise((_, reject) => setTimeout(() => reject(new Error("Connection timeout")), ms));
}
// ─────────────────────────────────────────────
// RabbitMQ Publisher
// ─────────────────────────────────────────────
async function publishMessage(amqpUrl, exchange, queue, routingKey, message, contentType) {
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
    }
    catch (err) {
        console.error("❌ Connection failed:", err);
    }
    finally {
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
        const amqpUrl = typeof data === "object" ? data?.amqp_config?.rabbitmq_host : null;
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
                await publishMessage(amqpUrl, exchange, queue, routingKey, rawPayload, contentType);
                break;
            }
            catch (e) {
                if (i === 2)
                    throw e;
            }
        }
    }
    catch (error) {
        if (error instanceof Error) {
            core.setFailed(`❌ ${error.message}`);
        }
    }
}
run();
//# sourceMappingURL=index.js.map
import redis from "redis";
import { config } from "../config";

class RedisServer {
    public client: redis.RedisClient;

    constructor() {
        this.initiateConnection();
    }

    public async initiateConnection(): Promise<void> {
        // this.client = redis.createClient(Number(config.config.REDIS_PORT), config.config.REDIS_HOST_WRITE);
        this.client = redis.createClient({ url: config.config.REDIS_CONN });

        this.client.on("connect", () => {
            console.log("Connected to redis client successfully");
        });
    }
}

const redisClient = new RedisServer().client;
export default redisClient;

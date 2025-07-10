import { config } from "../config";
import { kafkaHelper } from "../helpers";
import { WalletModel } from "../models/tables";
import utxo from "./utxo";

class updateBalanceProcess {
    public btc_pending_balance_update_topic = config.config.KAFKA_BALANCE.BTC_PENDING_BALANCE_UPDATE_TOPIC;
    public btc_pending_balance_update_group = config.config.KAFKA_BALANCE.BTC_PENDING_BALANCE_UPDATE_GROUP_ID;

    // public update_wallet_balance = async () => {
    public updateWalletBalance = async () => {
        let consumer: any = null;
        let isConnected = false;

        const connectConsumer = async () => {
            try {
                if (consumer) {
                    await consumer.disconnect();
                }

                consumer = await kafkaHelper.kafka.consumer({
                    groupId: this.btc_pending_balance_update_group,
                    // Add session timeout for better error handling
                    sessionTimeout: 30000,
                    // Add heartbeat interval
                    heartbeatInterval: 3000,
                });

                await consumer.subscribe({
                    topic: this.btc_pending_balance_update_topic,
                    fromBeginning: false, // ✅ Changed to false to prevent reprocessing
                });

                await consumer.run({
                    // ✅ Add proper autoCommit configuration
                    autoCommit: true,
                    autoCommitInterval: 5000,
                    autoCommitThreshold: 100,

                    eachMessage: async ({ topic, partition, message }: { topic: any; partition: any; message: any }) => {
                        const messageStartTime = Date.now();
                        try {
                            console.log(`📨 Processing message from topic: ${topic}, partition: ${partition}`);
                            this.updateBalance(JSON.parse(message.value.toString()))
                            const messageTime = Date.now() - messageStartTime;
                            console.log(`✅ Message processed successfully in ${messageTime}ms`);

                        } catch (error) {
                            const messageTime = Date.now() - messageStartTime;
                            console.error(`❌ Error processing message:`, error);
                            console.error(`⏱️  Time elapsed before error: ${messageTime}ms`);

                            // ✅ Better error recovery - don't add fixed delays
                            // The optimized parseSolanaTransaction already has proper error handling
                        }
                    },
                });

                isConnected = true;
                console.log(`✅ Consumer connected successfully to topic: ${this.btc_pending_balance_update_topic}`);
                console.log(`📊 Consumer group ID: ${this.btc_pending_balance_update_group}`);

            } catch (error) {
                console.error(`❌ Failed to connect consumer:`, error);
                isConnected = false;
                throw error;
            }
        };

        // ✅ Add reconnection logic
        const startWithReconnection = async () => {
            while (true) {
                try {
                    if (!isConnected) {
                        console.log(`🔄 Attempting to connect consumer...`);
                        await connectConsumer();
                    }

                    // Keep the consumer alive
                    await new Promise(resolve => setTimeout(resolve, 30000));

                } catch (error) {
                    console.error(`❌ Consumer connection error:`, error);
                    isConnected = false;

                    // Wait before retrying
                    console.log(`⏳ Waiting 10 seconds before reconnecting...`);
                    await new Promise(resolve => setTimeout(resolve, 10000));
                }
            }
        };

        // Start the consumer with reconnection
        await startWithReconnection();
    };

    // public update_wallet_balance = async () => {
    //     let consumer = await kafkaHelper.kafka.consumer({ groupId: this.btc_pending_balance_update_group });

    //     await consumer.subscribe({ topic: this.btc_pending_balance_update_topic, fromBeginning: true });

    //     await consumer.run({
    //         eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
    //             this.updateBalance(JSON.parse(message.value.toString()))
    //         }
    //     }).catch((err: Error) => {
    //         console.log("🔥 ~ startBlockTxQueue consumer error", err);
    //     });
    // };

    public updateBalance = async (data: any) => {
        try {
            if (data.queue_count < 10) {
                let balance = await utxo.getUserBtcBalance(data.wallet_address)

                if (balance !== null) {
                    await WalletModel.update(
                        { balance: Number(balance) },
                        { where: { wallet_address: data.wallet_address, coin_id: data.coin_data.coin_id } })
                } else {
                    await kafkaHelper.produceMessage(this.btc_pending_balance_update_topic, {
                        coin_data: data.coin_data,
                        wallet_address: data.wallet_address,
                        queue_count: data.queue_count + 1
                    })
                }
            }

        } catch (err: any) {
            console.error("Error in updateBalance", err)
        }
    }



}


const pendingBalanceUpdateProcess = new updateBalanceProcess();


export default pendingBalanceUpdateProcess;
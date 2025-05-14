import { config } from "../../config";
import { Kafka_Helper } from "../../helpers";
import { WalletModel } from "../../models/tables";
import utxo from "./utxo";

class UpdateBalanceController {
    public btc_pending_balance_update_topic = config.config.KAFKA_BALANCE.BTC_PENDING_BALANCE_UPDATE_TOPIC;
    public btc_pending_balance_update_group = config.config.KAFKA_BALANCE.BTC_PENDING_BALANCE_UPDATE_GROUP_ID;

    public update_wallet_balance = async () => {
        let consumer = await Kafka_Helper.kafka.consumer({ groupId: this.btc_pending_balance_update_group });

        await consumer.subscribe({ topic: this.btc_pending_balance_update_topic, fromBeginning: true });

        await consumer.run({
            eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
                this.updateBalance(JSON.parse(message.value.toString()))
            }
        }).catch((err: Error) => {
            console.log("🔥 ~ startBlockTxQueue consumer error", err);
        });
    };

    public updateBalance = async (data: any) => {
        try {
            if (data.queue_count < 10) {
                let balance = await utxo.getUserBtcBalance(data.wallet_address)

                if (balance !== null) {
                    await WalletModel.update(
                        { balance: Number(balance) },
                        { where: { wallet_address: data.wallet_address, coin_id: data.coin_data.coin_id } })
                } else {
                    await Kafka_Helper.produceMessage(this.btc_pending_balance_update_topic, {
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


const pendingBalanceUpdateBtc = new UpdateBalanceController();



export default pendingBalanceUpdateBtc;
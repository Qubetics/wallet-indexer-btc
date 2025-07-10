import { BooleanEnum } from "../enum";
import {
    DeviceTokenModel,
    WalletModel
} from "../models/tables";
import { CoinInterface } from "../models/interfaces";
import { config } from "../config/config";
import { kafkaHelper } from "./kafka.helper";
import { Op } from "sequelize";
class DbHelper {
    public btc_pending_balance_update_topic = config.KAFKA_BALANCE.BTC_PENDING_BALANCE_UPDATE_TOPIC;

    // Update_Balance = async (
    updateBalance = async (
        wallet_address: string,
        coin: CoinInterface,
        balance: number
    ) => {
        try {
            if (!wallet_address) {
                console.error(`wallet_address missing >>> `);
                return false;
            }

            if (balance !== undefined && balance !== null) {
                await WalletModel.update({
                    balance: balance,
                    status: 1
                }, {
                    where: {
                        wallet_address: wallet_address,
                        coin_id: coin.coin_id
                    }
                });
                return true;
            } else {
                const data = {
                    coin_data: coin,
                    wallet_address: wallet_address,
                    queue_count: 0,
                };
                await kafkaHelper.produceMessage(this.btc_pending_balance_update_topic, data);
                return true;
            }
        } catch (error: any) {
            console.error(`WalletHelper updateBalance error >>>`, error);
            return false;
        }
    }
    /** get user device token using user id */
    getDeviceTokens = async (user_id: number, address: string, coin_family: number) => {
        try {
            return await DeviceTokenModel.findAll({
                attributes: ["device_token"],
                where: {
                    push: BooleanEnum.true,
                    address: {
                        [Op.like]: address,
                    },
                    coin_family: coin_family,
                },
                order: [
                    ["updated_at", "DESC"],
                    ['id', 'DESC']
                ],
                raw: true
            });
        } catch (error: any) {
            console.error(`getDeviceToken error >>>`, error);
            return false;
        }
    }
}
export let dbHelper = new DbHelper();


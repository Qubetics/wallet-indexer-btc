// import Sequelize from "sequelize";
// import database from "../../config/db";
import { config } from "../../config";
import { Kafka_Helper } from "../../helpers/index";
import { TrnxHistoryModel } from "../../models/tables";
import { Op } from "sequelize";

class BTCPendingWithdrawalProcess {
  public btc_pending_withdrawal_tx_topic = config.config.KAFKA_TRANSACTIONS.BTC_PENDING_WITHDRAWAL_TX_TOPIC;

  public getTransactionFromDB = async () => {
    try {
      let tx: any = await TrnxHistoryModel.findAll({
        attributes: ["tx_id", "coin_id"],
        where: {
          status: 'completed',
          [Op.or]: [{ blockchain_status: null }, { blockchain_status: 'pending' }],
          req_type: { [Op.notIn]: ['TRANSAK', 'ALCHEMY'] },
          coin_family: config.config.STATIC_COIN_FAMILY.BTC,
          tx_id: { [Op.not]: null }
        },
        raw: true
      })
      if (tx.length > 0) {
        for await (const el of tx) {
          console.log("el>>>", el)
          await this.addTxToQueue(el.tx_id);
        }
      }
    } catch (error) {
      console.error('getTransactionFromDB error', error);
    }
  };

  public addTxToQueue = async (txId: string) => {
    try {

      await Kafka_Helper.produceMessage(this.btc_pending_withdrawal_tx_topic, { txId: txId });
      // await RabbitMq_Helper.assertQueue(config.config.PENDING_WITHDRAWAL_TX_PROCESS_BTC);
      // await RabbitMq_Helper.sendToQueue(config.config.PENDING_WITHDRAWAL_TX_PROCESS_BTC, Buffer.from(JSON.stringify({ txId: txId })));
    } catch (error) {
      console.error('addTxToQueue error', error);
    }
  };
}

const btcPendingWithdrawalProcess = new BTCPendingWithdrawalProcess();
export default btcPendingWithdrawalProcess;

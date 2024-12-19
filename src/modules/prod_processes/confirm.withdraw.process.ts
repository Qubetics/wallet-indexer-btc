import { config } from '../../config/config';
import utxo from './utxo';
import { Db_Helper, Kafka_Helper, Utility_Helper } from '../../helpers';
import { TrnxHistoryModel } from '../../models/tables';
import { Op } from 'sequelize';
import { BlockChainStatusEnum, NotificationTypeEnum, TrnxTypeEnum } from '../../enum/index';
import btcProcessHelper from '../prod_processes/process.helper';
import { title } from 'process';
class BTCTxStatusUpdateProcess {

  public startTxStatusUpdateQueue = async () => {
    let consumer = await Kafka_Helper.kafka.consumer({ groupId: config.KAFKA_TRANSACTIONS.BTC_PENDING_WITHDRAWAL_TX_GROUP_ID });
    await consumer.subscribe({ topic: config.KAFKA_TRANSACTIONS.BTC_PENDING_WITHDRAWAL_TX_TOPIC, fromBeginning: true });

    await consumer.run({
      eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
        // console.log("🚀 ~ startBlockTxQueue topic", topic);
        // console.log("🚀 ~ startBlockTxQueue message.value", message.value.toString());
        this.getTx(JSON.parse(message.value.toString()))
      }
    }).catch((err: Error) => {
      console.log("🔥 ~ startBlockTxQueue consumer error", err);
    });
  };

  // public startTxStatusUpdateQueue = async () => {
  //   try {
  //     await RabbitMq_Helper.consumeQueue(
  //       config.PENDING_WITHDRAWAL_TX_PROCESS_BTC || "",
  //       this.getTx
  //     );
  //   } catch (error: any) {
  //     console.error('startTxStatusUpdateQueue >>>>', error);
  //   }
  // };
  public getTx = async (data: { txId: string }) => {
    try {
      await utxo.getTransactionById(data.txId).then(async (rawData) => {
        if (
          rawData.confirmations != undefined &&
          rawData.confirmations >=
          Number(config.MIN_BTC_TRANSACTION_CONFORMATIONS)
        ) {
          const txUpdate = {
            status: 'success',
            txid: data.txId,
          };
          await this.sendTxUpdate(txUpdate);
        } else {
          return null;
        }
      });
    } catch (error: any) {
      console.error('getTx error', error);
    }
  };

  public sendTxUpdate = async (txData: {
    status: string;
    txid: string;
  }) => {
    try {
      const txnData: any = await TrnxHistoryModel.findOne({ attributes: ['from_adrs', 'coin_id', 'amount', 'user_id', 'id', 'to_adrs', 'type', 'coin_family'], where: { tx_id: txData.txid, status: 'completed', [Op.or]: [{ blockchain_status: { [Op.is]: null as any } }, { blockchain_status: 'pending' }] }, raw: true });
      if (txnData) {
        let coinData: any = await btcProcessHelper.CoinByCoinId(txnData.coin_id)
        let from_address = txnData.from_adrs;
        let coin_id: number = txnData.coin_id;
        let amount = txnData.amount;
        let user_id = txnData.user_id;
        let tx_row_id = txnData.id;
        let status_tx = txData.status == "success" ? BlockChainStatusEnum.CONFIRMED : BlockChainStatusEnum.FAILED;
        let to_address = txnData.to_adrs;
        let tx_type = txnData?.type
        let coin_symbol = coinData?.coin_symbol
        let coin_family = txnData.coin_family;
        let txUpdateRes = await TrnxHistoryModel.update({ blockchain_status: status_tx }, { where: { id: tx_row_id } })
        if (txUpdateRes) {
          if (status_tx == BlockChainStatusEnum.CONFIRMED) {
            let coin_data: any = { coin_id: coin_id, coin_family: coin_family };
            let AddressWithdraw: any = await btcProcessHelper.check_our_wallet_address(from_address);
            if (AddressWithdraw) {

              let fetchBalance = await utxo.getUserBtcBalance(from_address);

              let balance: number | any = fetchBalance ? Number(fetchBalance) : null;
              console.log('from address balance >>>>>', balance);
              /** update withdraw user eth balance */
              /** Update User Btc Balance */
              await Db_Helper.Update_Balance(from_address, coin_data, balance);
              let trnxTypeW: string = "Withdraw";
              // switch (tx_type) {
              //   case TrnxTypeEnum.DAPP:
              //     trnxTypeW = "Smart Contract Execution";
              //     break;
              //   case TrnxTypeEnum.APPROVE:
              //     trnxTypeW = "Approval";
              //     break;
              //   case TrnxTypeEnum.SWAP:
              //     trnxTypeW = "Swap";
              //     break;
              //   case TrnxTypeEnum.CROSS_CHAIN:
              //     trnxTypeW = "Cross-chain Swap";
              //     break;
              //   case TrnxTypeEnum.WITHDRAW:
              //     trnxTypeW = "Withdraw";
              //     break;
              //   default:
              //     break;
              // }
              const notiMsg = `${trnxTypeW} of ${await Utility_Helper.bigNumberSafeConversion(
                amount
              )} ${coin_symbol.toUpperCase()} has been confirmed.`;
              let notifData: any = {
                title: trnxTypeW.toUpperCase(),
                notification_type: trnxTypeW,
                message: notiMsg,
                from_user_id: 0,
                amount: amount,
                to_user_id: user_id,
                coin_symbol: coin_symbol,
                wallet_address: from_address,
                tx_id: tx_row_id,
                coin_id: coin_id,
                coin_family: coin_family,
                tx_type: tx_type,
                state: "confirmed"
              };
              Utility_Helper.SendNotification(notifData);
            }

            let Addressdeposit: any = await btcProcessHelper.check_our_wallet_address(to_address);
            if (Addressdeposit) {
              let to_user_id: number = Addressdeposit ? Addressdeposit.user_id : 0;
              let trnxTypeD: string = "Deposit";
              let fetchBalance = await utxo.getUserBtcBalance(from_address);
              let balance: number | any = fetchBalance ? Number(fetchBalance) : null;
              console.log('to address balance >>>>>', balance);
              /** update to adrs wallet balance */
              await Db_Helper.Update_Balance(to_address, coin_data, balance);
              const notiMsg = `${trnxTypeD} of ${await Utility_Helper.bigNumberSafeConversion(
                amount
              )} ${coin_symbol.toUpperCase()} has been confirmed.`;

              let notifData: any = {
                title: trnxTypeD.toUpperCase(),
                message: notiMsg,
                amount: amount,
                from_user_id: user_id,
                to_user_id: to_user_id,
                wallet_address: to_address,
                tx_id: tx_row_id,
                coin_symbol: coin_symbol,
                coin_id: coin_id,
                coin_family: coin_family,
                tx_type: tx_type,
                notification_type: NotificationTypeEnum.DEPOSIT,
                state: "confirmed"
              };
              Utility_Helper.SendNotification(notifData);
            }

            return true;
          } else {
            let trnxTypeW: string = "Withdraw";
            // switch (tx_type) {
            //   case TrnxTypeEnum.DAPP:
            //     trnxTypeW = "Smart Contract Execution";
            //     break;
            //   case TrnxTypeEnum.APPROVE:
            //     trnxTypeW = "Approval";
            //     break;
            //   case TrnxTypeEnum.SWAP:
            //     trnxTypeW = "Swap";
            //     break;
            //   case TrnxTypeEnum.CROSS_CHAIN:
            //     trnxTypeW = "Cross-chain Swap";
            //     break;
            //   default:
            //     break;
            // }

            let notiMsg = `${trnxTypeW} request of ${await Utility_Helper.bigNumberSafeConversion(amount)} ${coin_symbol?.toUpperCase()} has been failed.`;
            let notifData: any = {
              title: trnxTypeW.toUpperCase(),
              message: notiMsg,
              amount: amount,
              from_user_id: 0,
              to_user_id: user_id,
              wallet_address: from_address,
              tx_id: tx_row_id,
              coin_symbol: coin_symbol,
              coin_id: coin_id,
              coin_family: coin_family,
              tx_type: tx_type,
              notification_type: NotificationTypeEnum.WITHDRAW,
              state: "failed"
            };
            Utility_Helper.SendNotification(notifData);
            // return false;
          }
        } else {
          return false;
        }
      } else {
        return false;
      }

    } catch (error: any) {
      console.error("error in btc sendTxUpdate>>>>.", error)
    }
  };



}

const btcTxStatusUpdateProcess = new BTCTxStatusUpdateProcess();
export default btcTxStatusUpdateProcess;

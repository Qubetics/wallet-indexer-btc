import { config } from '../config/config';
import utxo from './utxo';
import { dbHelper, kafkaHelper, utilityHelper } from '../helpers';
import { TrnxHistoryModel } from '../models/tables';
import { Op } from 'sequelize';
import { BlockChainStatusEnum, TrnxTypeEnum } from '../enum/index';
import btcProcessHelper from './process.helper';

class BtcTxStatusUpdateProcess {

  public startTxStatusUpdateQueue = async () => {
    let consumer: any = null;
    let isConnected = false;

    const connectConsumer = async () => {
      try {
        if (consumer) {
          await consumer.disconnect();
        }

        consumer = await kafkaHelper.kafka.consumer({
          groupId: config.KAFKA_TRANSACTIONS.BTC_PENDING_WITHDRAWAL_TX_GROUP_ID,
          // Add session timeout for better error handling
          sessionTimeout: 30000,
          // Add heartbeat interval
          heartbeatInterval: 3000,
        });

        await consumer.subscribe({
          topic: config.KAFKA_TRANSACTIONS.BTC_PENDING_WITHDRAWAL_TX_TOPIC,
          fromBeginning: true, // ✅ Changed to false to prevent reprocessing
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
              this.getTx(JSON.parse(message.value.toString()))
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
        console.log(`✅ Consumer connected successfully to topic: ${config.KAFKA_TRANSACTIONS.BTC_PENDING_WITHDRAWAL_TX_TOPIC}`);
        console.log(`📊 Consumer group ID: ${config.KAFKA_TRANSACTIONS.BTC_PENDING_WITHDRAWAL_TX_GROUP_ID}`);

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

  // public startTxStatusUpdateQueue = async () => {
  //   let consumer = await kafkaHelper.kafka.consumer({ groupId: config.KAFKA_TRANSACTIONS.BTC_PENDING_WITHDRAWAL_TX_GROUP_ID });
  //   await consumer.subscribe({ topic: config.KAFKA_TRANSACTIONS.BTC_PENDING_WITHDRAWAL_TX_TOPIC, fromBeginning: true });

  //   await consumer.run({
  //     eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
  //       this.getTx(JSON.parse(message.value.toString()))
  //     }
  //   }).catch((err: Error) => {
  //     console.log("🔥 ~ startBlockTxQueue consumer error", err);
  //   });
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
        const coinData: any = await btcProcessHelper.checkCoinData('Native')
        const fromAddress = txnData.from_adrs;
        const coinId: number = txnData.coin_id;
        const amount = txnData.amount;
        const userId = txnData.user_id;
        const txRowId = txnData.id;
        const statusTx = txData.status == "success" ? BlockChainStatusEnum.CONFIRMED : BlockChainStatusEnum.FAILED;
        const toAddress = txnData.to_adrs;
        const txType = txnData?.type
        const coinSymbol = coinData?.coin_symbol
        const coinFamily = txnData.coin_family;
        const txUpdateRes = await TrnxHistoryModel.update({ blockchain_status: statusTx }, { where: { id: txRowId } })
        if (txUpdateRes) {
          if (statusTx == BlockChainStatusEnum.CONFIRMED) {
            const coin_data: any = { coin_id: coinId, coin_family: coinFamily };

            const addresses = [fromAddress, toAddress].filter(Boolean);
            const results = await btcProcessHelper.getMultipleKeyValuePairs(`${config.BTC_WALLET_ADDRESSES}`, addresses);
            // const Addressdeposit: any = await btcProcessHelper.checkOurWalletAddress(to_address);
            const [addressWithdraw, addressDeposit]: any = results as string[];

            // let AddressWithdraw: any = await btcProcessHelper.checkOurWalletAddress(fromAddress);
            if (addressWithdraw) {
              const fetchBalance = await utxo.getUserBtcBalance(fromAddress);
              const balance: number | any = fetchBalance ? Number(fetchBalance) : null;
              console.log('from address balance >>>>>', balance);
              /** update withdraw user eth balance */
              /** Update User Btc Balance */
              await dbHelper.updateBalance(fromAddress, coin_data, balance);

              let trnxTypeW: string = "Withdraw";
              switch (txType) {
                case TrnxTypeEnum.SWAP:
                  trnxTypeW = "Swap";
                  break;
                case TrnxTypeEnum.CROSS_CHAIN:
                  trnxTypeW = "Cross Chain Swap";
                  break;
                default:
                  break;
              }
              const notiMsg = `${trnxTypeW} of ${await utilityHelper.bigNumberSafeConversion(
                amount
              )} ${coinSymbol.toUpperCase()} has been confirmed.`;
              const notifData: any = {
                title: trnxTypeW.toUpperCase(),
                notification_type: trnxTypeW,
                message: notiMsg,
                from_user_id: 0,
                amount: amount,
                to_user_id: userId,
                coin_symbol: coinSymbol,
                wallet_address: fromAddress,
                tx_id: txRowId,
                coin_id: coinId,
                coin_family: coinFamily,
                tx_type: txType,
                // state: "confirmed"
              };
              utilityHelper.sendNotification(notifData);
            }

            // let addressDeposit: any = await btcProcessHelper.checkOurWalletAddress(toAddress);
            if (addressDeposit) {
              const toUserId: number = addressDeposit ? JSON.parse(addressDeposit)?.user_id : 0;
              const trnxTypeD: string = "Deposit";
              const fetchBalance = await utxo.getUserBtcBalance(toAddress);
              const balance: number | any = fetchBalance ? Number(fetchBalance) : null;
              console.log('to address balance >>>>>', balance);
              /** update to adrs wallet balance */
              await dbHelper.updateBalance(toAddress, coin_data, balance);
              const notiMsg = `${trnxTypeD} of ${await utilityHelper.bigNumberSafeConversion(
                amount
              )} ${coinSymbol.toUpperCase()} has been confirmed.`;

              const notifData: any = {
                title: trnxTypeD.toUpperCase(),
                message: notiMsg,
                amount: amount,
                from_user_id: userId,
                to_user_id: toUserId,
                wallet_address: toAddress,
                tx_id: txRowId,
                coin_symbol: coinSymbol,
                coin_id: coinId,
                coin_family: coinFamily,
                tx_type: txType,
                notification_type: trnxTypeD,
                // state: "confirmed"
              };
              utilityHelper.sendNotification(notifData);
            }

            return true;
          } else {
            let trnxTypeW: string = "Withdraw";
            const notiMsg = `${trnxTypeW} of ${await utilityHelper.bigNumberSafeConversion(amount)} ${coinSymbol?.toUpperCase()} has been failed.`;
            const notifData: any = {
              title: trnxTypeW.toUpperCase(),
              message: notiMsg,
              amount: amount,
              from_user_id: 0,
              to_user_id: userId,
              wallet_address: fromAddress,
              tx_id: txRowId,
              coin_symbol: coinSymbol,
              coin_id: coinId,
              coin_family: coinFamily,
              tx_type: txType,
              notification_type: trnxTypeW,
              // state: "failed"
            };
            utilityHelper.sendNotification(notifData);
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

const btcTxStatusUpdateProcess = new BtcTxStatusUpdateProcess();
export default btcTxStatusUpdateProcess;

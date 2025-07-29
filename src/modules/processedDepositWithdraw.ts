import { config } from '../config/config';
import { kafkaHelper } from '../helpers/index';
import { TrnxHistoryModel } from '../models/tables/index';
import { Op } from 'sequelize';
import { BlockChainStatusEnum, TxReqTypesEnum } from '../enum/index';
import { TrnxTypeEnum } from '../enum/index';
import btcProcessHelper from './process.helper';
import { NotificationTypeEnum } from '../enum';
import { CoinInterface } from '../models/interfaces';
import utxo from './utxo';
import { dbHelper, utilityHelper } from '../helpers/index';
class confirmWithdrawDepositProcess {
  webhook: string;

  public startDepositQueue = async () => {
    let consumer: any = null;
    let isConnected = false;

    const connectConsumer = async () => {
      try {
        if (consumer) {
          await consumer.disconnect();
        }

        consumer = await kafkaHelper.kafka.consumer({
          groupId: config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_GROUP_ID,
          // Add session timeout for better error handling
          sessionTimeout: 30000,
          // Add heartbeat interval
          heartbeatInterval: 3000,
        });

        await consumer.subscribe({
          topic: config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC,
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
              this.getTransaction(JSON.parse(message.value.toString()))
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
        console.log(`✅ Consumer connected successfully to topic: ${config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC}`);
        console.log(`📊 Consumer group ID: ${config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_GROUP_ID}`);

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
  // public startDepositQueue = async () => {
  //   let consumer = await kafkaHelper.kafka.consumer({ groupId: config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_GROUP_ID });
  //   await consumer.subscribe({ topic: config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC, fromBeginning: true });

  //   await consumer.run({
  //     eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
  //       this.getTransaction(JSON.parse(message.value.toString()))
  //     }
  //   }).catch((err: Error) => {
  //     console.log("🔥 ~ startBlockTxQueue consumer error", err);
  //   });
  // };

  public getTransaction = async (data: {
    tx_id: string;
    from_address: string;
    to_address: string;
    amount: any;
    tx_type: string;
    block_id: number;
    tx_fee: number
  }) => {
    try {
      if (data.from_address?.toLocaleLowerCase() == data.to_address?.toLocaleLowerCase()) {
        console.error(`Transaction ${data.from_address}==${data.to_address}.`);
        return console.error(`Transaction ${data.from_address}==${data.to_address}.`);;
      }
      let tx_fee: any = (parseFloat(data.tx_fee.toString()) / 100000000).toString();
      let coin_data: any = await btcProcessHelper.checkCoinData('Native');
      const webhookTx: any = {
        coin: coin_data,
        tx_id: data.tx_id,
        to_address: data.to_address,
        from_address: data.from_address,
        block_id: data.block_id,
        amount: data.amount,
        status: 'completed',
        tx_fee: tx_fee,
        blockchain_status: BlockChainStatusEnum.CONFIRMED,
        txType: data.tx_type
      };
      this.updateTransaction(webhookTx);
    } catch (error: any) {
      return console.error('getTransaction error', error);
    }
  };

  public updateTransaction = async (data: {
    tx_id: string;
    from_address: string;
    to_address: string,
    amount: any;
    block_id: number;
    txType: string;
    coin: CoinInterface,
    status: string,
    tx_fee: number,
    blockchain_status: BlockChainStatusEnum
  }) => {

    try {
      const txHash: string = data.tx_id;
      const toAddress: string = data.to_address;
      const fromAddress: string = data.from_address;
      const amount: number = data.amount;
      const status: string = data.status;
      const blockchainStatus: BlockChainStatusEnum = data.blockchain_status;
      let txType: any = data.txType ? data.txType : TrnxTypeEnum.DEPOSIT;
      const fiat_currency = "USD";
      if (data.txType == TrnxTypeEnum.DEPOSIT) {
        console.log("DEPOSIT")
        const checkOldTrnx: any = await TrnxHistoryModel.findOne({
          where: {
            tx_id: {
              [Op.like]: txHash,
            },
            to_adrs: { [Op.like]: toAddress },
            req_type: { [Op.notIn]: [TxReqTypesEnum.ALCHEMY, TxReqTypesEnum.TRANSAK] }
          },
          raw: true,
        });
        if (checkOldTrnx) {
          console.error(`Transaction ${txHash} is already exists.`);
          return false;
        }
        if (checkOldTrnx && checkOldTrnx.tx_hash == txHash) {
          txType = checkOldTrnx.type;
        }
      } else {
        console.log("WITHDRAW")
        let checkOldTrnxFrom: any = await TrnxHistoryModel.findOne({
          where: {
            tx_id: {
              [Op.like]: txHash,
            },
            from_adrs: { [Op.like]: fromAddress },
            req_type: { [Op.notIn]: [TxReqTypesEnum.ALCHEMY, TxReqTypesEnum.TRANSAK] }
          },
          raw: true,
        });
        if (checkOldTrnxFrom) {
          console.error(`Transaction ${txHash} is already exist.`);
          return false;
        }
      }

      let userId = 0;
      let toUserId = 0;

      const addresses: string[] = fromAddress !== ''
        ? [fromAddress, toAddress].filter(Boolean)
        : [toAddress].filter(Boolean);

      const results = await btcProcessHelper.getMultipleKeyValuePairs(
        config.BTC_WALLET_ADDRESSES,
        addresses
      ) as (string | null)[];

      console.log('results >>>', results);

      // Helper for safe JSON parse
      const safeParse = (data: string | null): any => {
        try {
          return data ? JSON.parse(data) : null;
        } catch {
          return null;
        }
      };

      let withdrawAddressExist: string | null = null;
      let depositAddressExist: string | null = null;

      if (fromAddress !== '') {
        const [addressWithdraw, addressDeposit] = results;
        withdrawAddressExist = addressWithdraw;
        depositAddressExist = addressDeposit;

        const parsedWithdraw = safeParse(addressWithdraw);
        const parsedDeposit = safeParse(addressDeposit);

        userId = parsedWithdraw?.user_id ?? 0;
        toUserId = parsedDeposit?.user_id ?? 0;
      } else {
        const [addressDeposit] = results;
        depositAddressExist = addressDeposit;

        const parsedDeposit = safeParse(addressDeposit);
        toUserId = parsedDeposit?.user_id ?? 0;
      }


      const trx_data: any = {
        user_id: userId,
        to_user_id: toUserId,
        req_type: TxReqTypesEnum.EXNG,
        from_adrs: fromAddress,
        to_adrs: toAddress,
        coin_id: data.coin.coin_id,
        coin_family: data.coin.coin_family,
        amount: amount,
        status: status,
        block_id: data.block_id,
        tx_id: txHash,
        blockchain_status: blockchainStatus,
        type: txType,
        tx_fee: data.tx_fee ? data.tx_fee : null,
        fiat_type: fiat_currency
      }
      const newTrnx: any = await TrnxHistoryModel.create(trx_data)

      // let AddressWithdraw: any = await btcProcessHelper.checkOurWalletAddress(fromAddress);
      if (withdrawAddressExist) {
        const fetchBalance = await utxo.getUserBtcBalance(fromAddress);
        const balance: number | any = fetchBalance ? Number(fetchBalance) : null;
        console.log('from address balance >>>>>', balance);
        /** update withdraw user eth balance */
        /** Update User Btc Balance */
        await dbHelper.updateBalance(
          fromAddress, /// wallet_address,
          data.coin, /// coin
          balance
        );
        let trnxTypeW: string = "Withdraw";
        const notiMsg = `${trnxTypeW} of ${await utilityHelper.bigNumberSafeConversion(
          amount
        )} ${data.coin?.coin_symbol.toUpperCase()} has been confirmed.`;
        let notifData: any = {
          title: trnxTypeW.toUpperCase(),
          notification_type: trnxTypeW,
          message: notiMsg,
          from_user_id: 0,
          amount: amount,
          to_user_id: userId,
          coin_symbol: data.coin?.coin_symbol,
          wallet_address: fromAddress,
          tx_id: newTrnx.id,
          coin_id: data.coin.coin_id,
          coin_family: data.coin.coin_family,
          tx_type: trnxTypeW,
          // state: "confirmed"
        };
        utilityHelper.sendNotification(notifData);
      }

      if (depositAddressExist) {
        let trnxTypeD: string = "Deposit";
        let fetchBalance = await utxo.getUserBtcBalance(toAddress);
        let balance: number | any = fetchBalance ? Number(fetchBalance) : null;
        await dbHelper.updateBalance(
          toAddress, /// wallet_address,
          data.coin, /// coin
          balance
        );
        const notiMsg = `${trnxTypeD} of ${await utilityHelper.bigNumberSafeConversion(
          amount
        )} ${data.coin?.coin_symbol?.toUpperCase()} has been confirmed.`;

        let notifData: any = {
          title: NotificationTypeEnum.DEPOSIT?.toUpperCase(),
          message: notiMsg,
          from_user_id: userId,
          to_user_id: toUserId,
          wallet_address: toAddress,
          tx_id: newTrnx.id,
          coin_symbol: data.coin.coin_symbol,
          coin_family: data.coin.coin_family,
          coin_id: data.coin.coin_id,
          tx_type: txType,
          notification_type: trnxTypeD,
          // state: "confirmed",
          amount: amount
        };
        utilityHelper.sendNotification(notifData);
      }
      return true;
    } catch (err: any) {
      console.error(`SaveTrnx_Process-SaveNewTrnx error >>>`, err);
      return false;
    }
  };
}

const confirmDepositWithdrawProcess = new confirmWithdrawDepositProcess();
export default confirmDepositWithdrawProcess;

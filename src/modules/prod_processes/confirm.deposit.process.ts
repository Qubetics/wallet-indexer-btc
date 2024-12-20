import { config } from '../../config/config';
import { Kafka_Helper } from '../../helpers/index';
import { TrnxHistoryModel } from '../../models/tables/index';
import { Op } from 'sequelize';
import { BlockChainStatusEnum, TxReqTypesEnum } from '../../enum/index';
import { TrnxTypeEnum } from '../../enum/index';
import btcProcessHelper from './process.helper';
import { NotificationTypeEnum } from '../../enum';
import { CoinInterface, NotificationInterface } from '../../models/interfaces';
import utxo from './utxo';
import { Db_Helper, Utility_Helper } from '../../helpers/index';
class ConfirmDepositProcess {
  webhook: string;

  public startDepositQueue = async () => {
    let consumer = await Kafka_Helper.kafka.consumer({ groupId: config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_GROUP_ID });
    await consumer.subscribe({ topic: config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC, fromBeginning: true });

    await consumer.run({
      eachMessage: ({ topic, partition, message }: { topic: any, partition: any, message: any }) => {
        // console.log("🚀 ~ startBlockTxQueue topic", topic);
        // console.log("🚀 ~ startBlockTxQueue message.value", message.value.toString());
        this.getTransaction(JSON.parse(message.value.toString()))
      }
    }).catch((err: Error) => {
      console.log("🔥 ~ startBlockTxQueue consumer error", err);
    });
  };

  // public startDepositQueue = async () => {
  //   await RabbitMq_Helper.consumeQueue(
  //     config.DEPOSIT_WITHDRAW_PROCESS_BTC || "",
  //     this.getTransaction
  //   );
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
      let coin_data: any = await btcProcessHelper.NativeCoinByCoinFamily(config.STATIC_COIN_FAMILY.BTC);
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
      const tx_hash: string = data.tx_id;
      const to_address: string = data.to_address;
      const from_address: string = data.from_address;
      const amount: number = data.amount;
      const status: string = data.status;
      const blockchain_status: BlockChainStatusEnum = data.blockchain_status;
      let tx_type: any = data.txType ? data.txType : TrnxTypeEnum.DEPOSIT;
      const fiat_currency = "USD";
      if (data.txType == TrnxTypeEnum.DEPOSIT) {
        console.log("DEPOSIT")
        let checkOldTrnx: any = await TrnxHistoryModel.findOne({
          where: {
            tx_id: {
              [Op.like]: tx_hash,
            },
            to_adrs: to_address,
            req_type: { [Op.notIn]: [TxReqTypesEnum.ALCHEMY, TxReqTypesEnum.TRANSAK] }
          },
          raw: true,
        });
        if (checkOldTrnx) {
          console.error(`Transaction ${tx_hash} is already exists.`);
          return false;
        }
        if (checkOldTrnx && checkOldTrnx.tx_hash == tx_hash) {
          tx_type = checkOldTrnx.type;
        }
      } else {
        console.log("WITHDRAW")
        let checkOldTrnxFrom: any = await TrnxHistoryModel.findOne({
          where: {
            tx_id: {
              [Op.like]: tx_hash,
            },
            from_adrs: from_address,
            req_type: { [Op.notIn]: [TxReqTypesEnum.ALCHEMY, TxReqTypesEnum.TRANSAK] }
          },
          raw: true,
        });
        if (checkOldTrnxFrom) {
          console.error(`Transaction ${tx_hash} is already exist.`);
          return false;
        }
      }
      let Addressdeposit: any = await btcProcessHelper.check_our_wallet_address(to_address);
      let to_user_id: number = Addressdeposit ? Addressdeposit.user_id : 0;
      let trx_data: any = {
        user_id: 0,
        to_user_id: to_user_id,
        req_type: TxReqTypesEnum.EXNG,
        from_adrs: from_address,
        to_adrs: to_address,
        coin_id: data.coin.coin_id,
        coin_family: data.coin.coin_family,
        amount: amount,
        status: status,
        block_id: data.block_id,
        tx_id: tx_hash,
        blockchain_status: blockchain_status,
        type: tx_type,
        tx_fee: data.tx_fee ? data.tx_fee : null,
        fiat_type: fiat_currency
      }
      const newTrnx: any = await TrnxHistoryModel.create(trx_data)

      if (Addressdeposit) {
        let trnxTypeD: string = "Deposit";
        trnxTypeD = tx_type
        // switch (tx_type) {
        //   case TrnxTypeEnum.DAPP:
        //     trnxTypeD = "Smart Contract Execution";
        //     break;
        //   case TrnxTypeEnum.APPROVE:
        //     trnxTypeD = "Approval";
        //     break;
        //   case TrnxTypeEnum.SWAP:
        //     trnxTypeD = "Swap";
        //     break;
        //   case TrnxTypeEnum.CROSS_CHAIN:
        //     trnxTypeD = "Cross-chain Swap";
        //     break;
        //   case TrnxTypeEnum.DEPOSIT:
        //     trnxTypeD = "Deposit";

        //   default:
        //     break;
        // }

        let fetchBalance = await utxo.getUserBtcBalance(to_address);
        let balance: number | any = fetchBalance ? Number(fetchBalance) : null;
        await Db_Helper.Update_Balance(
          to_address, /// wallet_address,
          data.coin, /// coin
          balance
        );
        const notiMsg = `${trnxTypeD} of ${await Utility_Helper.bigNumberSafeConversion(
          amount
        )} ${data.coin?.coin_symbol?.toUpperCase()} has been confirmed.`;

        let notifData: any = {
          title: NotificationTypeEnum.DEPOSIT?.toUpperCase(),
          message: notiMsg,
          from_user_id: 0,
          to_user_id: to_user_id,
          wallet_address: to_address,
          tx_id: newTrnx.id,
          coin_symbol: data.coin.coin_symbol,
          coin_family: data.coin.coin_family,
          coin_id: data.coin.coin_id,
          tx_type: tx_type,
          notification_type: trnxTypeD,
          state: "confirmed",
          amount: amount
        };
        Utility_Helper.SendNotification(notifData);
      }
      return true;
    } catch (err: any) {
      console.error(`SaveTrnx_Process-SaveNewTrnx error >>>`, err);
      return false;
    }
  };
}

const confirmDepositProcess = new ConfirmDepositProcess();
export default confirmDepositProcess;

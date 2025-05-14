import { config } from '../../config/config';
import { LogsConstants } from '../../constants/logs.constants';
import utxo from './utxo';
import btcProcessHelper from './process.helper';
import { Kafka_Helper } from '../../helpers/index';
import { TrnxTypeEnum } from '../../enum/index';
// import { SettingsModel } from '../../models//tables/index';
class BTCBlocksProcess {
  public minBlockConfirmation: number;
  public depositWithdrawTopic = config.KAFKA_TRANSACTIONS.DEPOSIT_WITHDRAW_TOPIC;

  constructor() {
    this.minBlockConfirmation = Number(config.MIN_BLOCK_CONFORMATIONS);
  }
  public readBlocks = async () => {
    try {
      const latestBlock: any = await utxo.getLatestBlock();
      const currentBlock: number = parseInt(latestBlock);
      console.log('currentBlock >>>', currentBlock);
      const lastBlockToProcess: any = currentBlock - this.minBlockConfirmation;
      console.log('lastBlockToProcess >>>', lastBlockToProcess);


      const lastBlockProcessed: any = await btcProcessHelper.getBlockNumber(config.LAST_BLOCK_NUMBER_BTC || '');
      if (!lastBlockProcessed) {
        await btcProcessHelper.setRedisSting(config.LAST_BLOCK_NUMBER_BTC || '', lastBlockToProcess);
        return console.log('No block present in redis');
      }
      if (lastBlockProcessed <= lastBlockToProcess) {
        await btcProcessHelper.updateLastBlockProcessed(config.LAST_BLOCK_NUMBER_BTC || '', lastBlockProcessed);
        await this.getTxFromBlock(lastBlockProcessed);
      } else {
        return console.log(LogsConstants.BLOCK_NUMBER_EXCEEDING);
      }
    } catch (error: any) {
      console.error('readBlocks error', error);
    }
  };

  public getTxFromBlock = async (blockId: number) => {
    const blockHash: any = await utxo.getblockHash(blockId, 1);
    const totalPages: any = blockHash.totalPages;
    for (let i = 1; i <= totalPages; i++) {
      const blockHashInternal: any = await utxo.getblockHash(blockId, i);
      console.table({
        TotalPages: totalPages,
        CurrentPage: i,
        BlockHash: blockId,
      });
      const transactionsIds = blockHashInternal.txs;
      try {
        if (transactionsIds) {
          if (transactionsIds?.length > 0) {
            let _this = this;
            let from: any;
            transactionsIds.forEach(async function (iterator: any) {
              iterator.vout.forEach(async function (txData: any) {
                console.log('txData?.addresses.length >>>>>', txData?.addresses?.length);
                let to_address = txData !== null && txData?.addresses?.length > 0 && txData?.addresses[0] ? txData?.addresses[0]?.toUpperCase() : "";
                console.log('to_address >>>>>', to_address);
                let wallet_data = await btcProcessHelper.check_our_wallet_address(to_address);
                if (wallet_data) {
                  if (iterator?.vin[0]?.addresses) {
                    from = iterator?.vin[0]?.addresses[0];
                  }
                  let amount: any = (parseFloat(txData.value) / 100000000).toString();
                  let dataObj: {
                    tx_id: string;
                    from_address: string;
                    to_address: string;
                    amount: any;
                    tx_type: string;
                    block_id: number;
                    tx_fee: number
                  } = {
                    tx_id: iterator.txid,
                    from_address: from ? from : '',
                    // to_address: txData.addresses[0],
                    to_address: to_address,
                    amount: amount,
                    tx_type: TrnxTypeEnum.DEPOSIT,
                    block_id: blockId,
                    tx_fee: iterator.fees

                  };
                  await _this.addTxToQueue(
                    dataObj,
                    _this.depositWithdrawTopic || ''
                    //config.DEPOSIT_WITHDRAW_PROCESS_BTC || ''
                  );
                }

              })
            })
          }
        }
      } catch (error: any) {
        console.error('getTxFromBlock >>>>>', error);
      }
    }
  };

  public addTxToQueue = async (data: any, queueName: string) => {
    try {
      await Kafka_Helper.produceMessage(queueName, data);
      // await RabbitMq_Helper.assertQueue(queueName);
      // await RabbitMq_Helper.sendToQueue(queueName, Buffer.from(JSON.stringify(data)));
    } catch (error: any) {
      console.error('addTxToQueue error', (error as Error).message);
    }
  };
  public async readSpecificBlock() {
    try {
      console.log("Entere into readSpecificBlock>>>")
      let specificBlocks: any = await btcProcessHelper.getKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_BTC)
      let specific_block: any = JSON.parse(specificBlocks);
      if (specific_block && Number(specific_block) > 0) {
        console.table({ SPECIFIC_BLOCK: specific_block })
        await this.getTxFromBlock(specific_block)
        await btcProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_BTC, '0')
      } else {
        console.log("Specific block is not present>>>>")
        if (Number(specific_block) > 0) {
          await btcProcessHelper.setKeyValuePair(config.REDISKEYS.ALL_PROCESSES, config.REDISKEYS.SPECIFIC_BLOCK_BTC, '0')
        }
      }
    } catch (error: any) {
      console.error("Error in readSpecificBlock>>>", error)
    }

  }
  // public async testingNode() {
  //   try {
  //     // Get Block Number
  //     let getBlockInfo: any = await utxo.getLatestBlock();
  //     // console.debug("getBlockInfo >>>>>>>>>>>", getBlockInfo)
  //     let blockId: number = parseInt(getBlockInfo)

  //     // Get Block Info
  //     const blockHash: any = await utxo.getblockHash(blockId, 1);
  //     // console.log("blockInfo>>", blockHash.txs[0].txid)

  //     // Get Block Receipt
  //     const transaction: any = await utxo.getTransactionById(blockHash.txs[0].txid)
  //     // console.log("txreceipt>>", transaction)

  //     // Get Balance
  //     const balance: any = await utxo.getUserBtcBalance("bc1qexampleaddress")
  //     // console.log("balance>>", balance)

  //     // Get Node Info
  //     // const info: any = await utxo.getNodeInfo()
  //     // console.log("info>>", info)

  //   } catch (err: any) {
  //     console.error("Error in testingNode>>", err)
  //   }
  // }
}

const btcBlocksProcess = new BTCBlocksProcess();
export default btcBlocksProcess;

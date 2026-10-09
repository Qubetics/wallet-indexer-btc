import { config } from '../config/config';
import { LogsConstants } from '../constants/logs.constants';
import utxo from './utxo';
import btcProcessHelper from './process.helper';
import { kafkaHelper } from '../helpers/index';
import { TrnxTypeEnum } from '../enum/index';
class BtcBlockReadProcess {
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
        if (transactionsIds && transactionsIds.length > 0) {
          const txPromises = transactionsIds.map(async (iterator: any) => {
            let from: null | string = '';
            if (iterator.vout && iterator.vout.length > 0) {
              const voutPromises = iterator.vout.map(async (txData: any) => {
                const to_address = txData && txData.addresses && txData.addresses.length > 0 && txData.addresses[0] ? txData.addresses[0].toUpperCase() : "";
                if (iterator?.vin && iterator.vin[0]?.addresses) {
                  from = iterator.vin[0].addresses[0];
                }

                const addresses = [from, to_address].filter(Boolean);
                if (addresses.length > 0) {
                  const amount: any = (parseFloat(txData.value) / 100000000).toString();
                  const dataObj: {
                    tx_id: string;
                    from_address: string;
                    to_address: string;
                    amount: any;
                    tx_type: string | null;
                    block_id: number;
                    tx_fee: number
                  } = {
                    tx_id: iterator.txid,
                    from_address: from ? from : '',
                    to_address: to_address,
                    amount: amount,
                    tx_type: null,
                    block_id: blockId,
                    tx_fee: iterator.fees
                  };

                  const results: any = await btcProcessHelper.getMultipleKeyValuePairs(`${config.BTC_WALLET_ADDRESSES}`, addresses);
                  if (results) {
                    if (from !== '' && results?.length > 1) {
                      const [withdrawTx, depositTx] = results as string[];
                      if (depositTx) {
                        dataObj.tx_type = TrnxTypeEnum.DEPOSIT;
                      } else if (from !== '' && withdrawTx) {
                        dataObj.tx_type = TrnxTypeEnum.WITHDRAW;
                      }
                    } else {
                      const [depositTx] = results as string[];
                      if (depositTx) {
                        dataObj.tx_type = TrnxTypeEnum.DEPOSIT;
                      }
                    }
                  }

                  if (dataObj.tx_type !== null) {
                    console.log('dataObj >>>', dataObj);
                    await this.addTxToQueue(
                      dataObj,
                      this.depositWithdrawTopic || ''
                    );
                  }
                }

              });
              await Promise.all(voutPromises);
            }
          });
          await Promise.all(txPromises);
        }
      } catch (error: any) {
        console.error('getTxFromBlock >>>>>', error);
      }
    }
  };

  public addTxToQueue = async (data: any, queueName: string) => {
    try {
      await kafkaHelper.produceMessage(queueName, data);
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
}

const btcBlockReadProcess = new BtcBlockReadProcess();
export default btcBlockReadProcess;

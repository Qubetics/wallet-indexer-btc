import schedule from 'node-schedule';
import btcBlockReadProcess from './modules/processedBlocksRead';
import confirmDepositWithdrawProcess from './modules/processedDepositWithdraw';
import btcTxStatusUpdateProdProcess from './modules/processedInternalTransactionStatus';
import btcPendingWithdrawalProdProcess from './modules/processedInternalTransactions';
import pendingBalanceUpdateProcess from './modules/processedBalanceUpdate';
import { kafkaHelper } from './helpers';

class BTCStartProcess {
  constructor() {
    this.connectKafka();
    // this.readBlocksProcess();
    // this.confirmDepositProcess();
    this.pendingWithdrawalProcess();
    this.btcTxStatusUpdateProcess();
    // this.readSpecificBlock();
    // this.btcUpdatePendingTxBalance();
    // this.testing_node()

  }

  public async connectKafka() {
    await kafkaHelper.connect();
  }

  // public testing_node() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await btcBlocksProdProcess.testingNode();
  //   });
  // }

  public readBlocksProcess() {
    schedule.scheduleJob('*/15 * * * * *', async function () {
      try {
        await btcBlockReadProcess.readBlocks();
      } catch (error) {
        console.error('readBlocksProcess >>', error);
      }
    });
  }
  public readSpecificBlock() {
    schedule.scheduleJob("*/5 * * * *", async function () {
      await btcBlockReadProcess.readSpecificBlock();
    })
  }

  public pendingWithdrawalProcess() {
    schedule.scheduleJob('*/5 * * * * *', async function () {
      try {
        await btcPendingWithdrawalProdProcess.getTransactionFromDB();
      } catch (error) {
        console.error('pendingWithdrawalProcess >>', error);
      }
    });
  }

  public async confirmDepositProcess() {
    try {
      setTimeout(async () => {
        await confirmDepositWithdrawProcess.startDepositQueue();
      }, 10000);
    } catch (error) {
      console.error('confirmDepositProcess >>', error);
    }
  }

  public async btcTxStatusUpdateProcess() {
    try {
      setTimeout(async () => {
        await btcTxStatusUpdateProdProcess.startTxStatusUpdateQueue();
      }, 10000);
    } catch (error) {
      console.error('btcTxStatusUpdateProcess >>', error);
    }
  }

  public async btcUpdatePendingTxBalance() {
    try {
      setTimeout(async () => {
        await pendingBalanceUpdateProcess.updateWalletBalance();
      }, 10000);
    } catch (error) {
      console.error('btcUpdatePendingTxBalance >>', error);
    }
  }
}

new BTCStartProcess();

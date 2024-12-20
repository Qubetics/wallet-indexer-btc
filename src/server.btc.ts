import schedule from 'node-schedule';
import btcBlocksProdProcess from './modules/prod_processes/blocks.process';
import confirmDepositProdProcess from './modules/prod_processes/confirm.deposit.process';
import btcTxStatusUpdateProdProcess from './modules/prod_processes/confirm.withdraw.process';
import btcPendingWithdrawalProdProcess from './modules/prod_processes/withdraw.process';
import { config } from './config/config';
import pendingBalanceUpdateBtc from './modules/prod_processes/updating_pending_address_balance.process';




class BTCStartProcess {
  constructor() {
    this.readBlocksProcess();
    this.confirmDepositProcess();
    this.pendingWithdrawalProcess();
    this.btcTxStatusUpdateProcess();
    this.readSpecificBlock();
    this.btcUpdatePendingTxBalance();
    // this.testing_node()

  }


  // public testing_node() {
  //   schedule.scheduleJob("*/2 * * * * *", async function () {
  //     await btcBlocksProdProcess.testingNode();
  //   });
  // }

  public readBlocksProcess() {
    schedule.scheduleJob('*/15 * * * * *', async function () {
      try {
        // if (config.SERVER == 'stage' || config.SERVER == 'dev' || config.SERVER == 'prod') {
        await btcBlocksProdProcess.readBlocks();
        // }
      } catch (error) {
        console.error('readBlocksProcess >>', error);
      }
    });
  }
  public readSpecificBlock() {
    schedule.scheduleJob("*/5 * * * *", async function () {
      await btcBlocksProdProcess.readSpecificBlock();
    })
  }

  public pendingWithdrawalProcess() {
    schedule.scheduleJob('*/5 * * * * *', async function () {
      try {
        // if (config.SERVER == 'stage' || config.SERVER == 'dev' || config.SERVER == 'prod') {
        await btcPendingWithdrawalProdProcess.getTransactionFromDB();
        // }
      } catch (error) {
        console.error('pendingWithdrawalProcess >>', error);
      }
    });
  }

  public async confirmDepositProcess() {
    try {
      setTimeout(async () => {
        // if (config.SERVER == 'stage' || config.SERVER == 'dev' || config.SERVER == 'prod') {
        await confirmDepositProdProcess.startDepositQueue();
        // }
      }, 10000);
    } catch (error) {
      console.error('confirmDepositProcess >>', error);
    }
  }

  public async btcTxStatusUpdateProcess() {
    try {
      setTimeout(async () => {
        // if (config.SERVER == 'stage' || config.SERVER == 'dev' || config.SERVER == 'prod') {
        await btcTxStatusUpdateProdProcess.startTxStatusUpdateQueue();
        // }
      }, 10000);
    } catch (error) {
      console.error('btcTxStatusUpdateProcess >>', error);
    }
  }


  public async btcUpdatePendingTxBalance() {
    try {
      setTimeout(async () => {
        // if (config.SERVER == 'stage' || config.SERVER == 'dev' || config.SERVER == 'prod') {
        await pendingBalanceUpdateBtc.update_wallet_balance();
        // }
      }, 10000);
    } catch (error) {
      console.error('btcUpdatePendingTxBalance >>', error);
    }
  }
}

new BTCStartProcess();

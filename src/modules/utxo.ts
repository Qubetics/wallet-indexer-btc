import { config } from '../config';
import axios, { AxiosInstance } from "axios";
class UTXO {
  public config: any;
  private rpcConfig: any;
  constructor() {
    this.config = {
      method: 'get',
      headers: {
        'apikey': `${config.config.BTC_API_KEY}`,
        'Content-Type': 'application/json',
      }
    };
    this.rpcConfig = {
      method: 'post',
      url: `${config.config.BTC_TESTNET_RPC_URL}`,
      headers: {
          'Content-Type': 'application/json',
      }
    };
  }

  async getNodeInfo() {
    try {
      this.config.url = `${config.config.BTC_RPC_URL}`
      const response = await axios(this.config);
      return response.data;
    } catch (error: any) {
      console.error('getNodeInfo error', error);
    }
  }

  async getLatestBlock() {
    try {
      this.config.url = `${config.config.BTC_RPC_URL}api/`
      const response = await axios(this.config);
      return response.data.backend.blocks;
    } catch (error: any) {
      console.error('getLatestBlock error', error);
    }
  }

  async getblockHash(blockNumber: number, page: number) {
    try {
      this.config.url = `${config.config.BTC_RPC_URL}api/v2/block/${blockNumber}?page=${page}`
      const response = await axios(this.config);
      return response.data;
    } catch (error: any) {
      console.error('getblockHash error', error);
    }
  }

  async getTransactionById(transactionId: string) {
    try {
      this.config.url = `${config.config.BTC_RPC_URL}api/v2/tx/${transactionId}`
      const response = await axios(this.config);
      return response.data;
    } catch (error: any) {
      console.error('getTransactionById error', error);
    }
  }

  async getUserBtcBalance(address: string) {
    try {
      this.config.url = `${config.config.BTC_RPC_URL}api/v2/address/${address}`
      const response = await axios(this.config);
      return response.data.balance / 100000000;
    } catch (error: any) {
      console.error('error getUserBtcBalance >>>>>>', error);
      return null;
    }
  }

  private async makeRpcCall(method: string, params: any[] = []) {
    try {
        const requestData = {
            jsonrpc: '2.0',
            method: method,
            params: params,
            id: 1
        };

        const response = await axios({
            ...this.rpcConfig,
            data: requestData
        });

        if (response.data.error) {
            console.log("🚀 ~ utxo ~ makeRpcCall ~ err  :", { method , error: response.data.error })
            throw new Error(`RPC Error: ${response.data.error.message}`);
        }
        console.log("🚀 ~ utxo ~ makeRpcCall ~ response:", { method , result: response.data.result })

        return response.data.result;
    } catch (err: any) {
        console.error(`Error in RPC call ${method}:`, err);
        throw err;
    }
  }
  public async getRpcTransactionDetails(txid: string) {
    try {
        const transaction = await this.makeRpcCall('getrawtransaction', [txid, true]);
        return transaction;
    } catch (err: any) {
        console.error("Error in getTransactionDetails of BTC", err);
        return null;
    }
  }
}

const utxo = new UTXO();
export default utxo;

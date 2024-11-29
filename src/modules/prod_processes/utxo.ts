import { config } from '../../config';
import axios, { AxiosInstance } from "axios";
class UTXO {
  // private clientUrl = config.config.BTC_RPC_URL;
  // private axiosClient: AxiosInstance;
  public config: any;
  constructor() {
    this.config = {
      method: 'get',
      headers: {
        'apikey': `${config.config.BTC_API_KEY}`,
        'Content-Type': 'application/json',
      }
    };
    // this.axiosClient = axios.create({
    //   baseURL: this.clientUrl,
    //   headers: {
    //     "Content-Type": "application/json",
    //     apikey: "REDACTED"
    //   },
    // });
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
      //return (error as Error).message;
      return null;
    }
  }

}

var utxo = new UTXO();
export default utxo;

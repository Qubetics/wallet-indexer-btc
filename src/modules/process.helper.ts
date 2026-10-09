import { promisify } from 'util';
import redisClient from '../helpers/redis.helper';
import database from '../config/db';
import { QueryTypes } from 'sequelize';
import { config } from '../config/config';
import { CoinsModel } from '../models/tables';
class BtcProcessHelper {
  constructor() {
    this.fetchWalletAddress();
    this.fetchCoins();
  }
  public setRedisSting = async (key: string, value: number) => {
    try {
      redisClient.set(key, value.toString());
    } catch (error) {
      console.error('setRedisSting error', error);
    }
  }

  public getBlockNumber = async (key: string) => {
    try {
      const getAsync = promisify(redisClient.get).bind(
        redisClient
      );
      const value = await getAsync(key);
      if (value) return parseInt(value);
      return null;
    } catch (error) {
      console.error('getBlockNumber error', error);
      return null;
    }
  }

  public updateLastBlockProcessed = async (
    key: string,
    lastBlockProcessed: number
  ) => {
    try {
      const updatedBlockNumber = ++lastBlockProcessed;
      redisClient.set(key, updatedBlockNumber.toString());
    } catch (error) {
      console.error('updateLastBlockProcessed error', error);
    }
  }

  public getKeyValuePair = async (keyName: string, field: string) => {
    try {
      const getAsync = promisify(redisClient.hget).bind(
        redisClient
      );
      let value: any = await getAsync(keyName, field.toUpperCase() || '');
      if (value) {
        return value;
      } else {
        return null
      }
    } catch (error: any) {
      console.error('getKeyValuePair error', error);
      return null;
    }
  }

  public setKeyValuePair = async (
    keyName: string,
    field: string,
    value: string
  ) => {
    try {
      redisClient.hset(
        keyName,
        field.toUpperCase(),
        value
      );
    } catch (error) {
      console.error('setKeyValuePair error', error);
    }
  }


  public fetchCoins = async () => {
    try {
      const coin: any = await CoinsModel.findOne({
        attributes: ["coin_id", "coin_symbol", "coin_family", "is_token", "token_type", "decimals", "token_address", "for_swap", "added_by"],
        where: {
          coin_status: 1,
          coin_family: config.STATIC_COIN_FAMILY.BTC
        },
        raw: true,
      });
      if (coin) {
        const findTokenData = await this.getKeyValuePair(
          `${config.TOKEN_TYPE_BTC}`,
          coin.token_address == null ? 'NativeCoin'.toUpperCase() : coin.token_address.toUpperCase(),
        );
        if (!findTokenData) {
          this.setKeyValuePair(
            `${config.TOKEN_TYPE_BTC}`,
            coin.token_address == null ? 'Native'.toUpperCase() : coin.token_address.toUpperCase(),
            JSON.stringify(coin)
          );
        }
      }
    } catch (err: any) {
      console.error('fetchCoins >>>>>', err);
    }
  }

  public checkCoinData = async (address: string) => {
    try {
      const token_data: any = await this.getKeyValuePair(config.TOKEN_TYPE_BTC, address);
      return JSON.parse(token_data);
    } catch (err: any) {
      console.error('checkCoinData >>>>>', err);
      return null;
    }
  };

  public fetchWalletAddress = async () => {
    try {
      const getaddressQuery = `SELECT ws.user_id,ws.wallet_address as address, ws.wallet_name FROM wallets ws FORCE INDEX (coin_family) JOIN coins cs ON ws.coin_id = cs.coin_id WHERE cs.coin_family = ${config.STATIC_COIN_FAMILY.BTC} GROUP BY ws.wallet_address`;
      const addresses: any = await database.db_write.query(
        getaddressQuery,
        { type: QueryTypes.SELECT }
      );
      for await (let wallet of addresses) {
        let findAddress = await this.getKeyValuePair(
          `${config.BTC_WALLET_ADDRESSES}`,
          wallet.address.toUpperCase()
        );
        if (!findAddress) {
          this.setKeyValuePair(
            `${config.BTC_WALLET_ADDRESSES}`,
            wallet.address.toUpperCase(),
            JSON.stringify(wallet)
          );
        }
      }
    } catch (error) {
      console.error('💥 ~ fetchWalletAddress error', error);
    }
  };

  public checkOurWalletAddress = async (address: string) => {
    try {
      let wallet_data: any = await this.getKeyValuePair(config.BTC_WALLET_ADDRESSES, address)
      return JSON.parse(wallet_data);
    } catch (error: any) {
      console.error('checkOurWalletAddress >>>>>', error);
      return null;
    }
  }


  public async getMultipleKeyValuePairs(keyName: string, fields: string[]) {
    try {
      const upperFields = fields.map(f => f.toUpperCase());
      return new Promise((resolve, reject) => {
        redisClient.hmget(keyName, ...upperFields, (err, results) => {
          if (err) reject(err);
          else resolve(results);
        });
      });
    } catch (err: any) {
      console.error('getMultipleKeyValuePairs >>>>', err);
      return null;
    }
  }
}

const btcProcessHelper = new BtcProcessHelper();
export default btcProcessHelper;

import zookeeperConfig from "../helpers/zookeeper.helper";
const name = 'qubetics';
export const config = {
  DB_USER: zookeeperConfig.config.DB_USER,
  DB_HOST_READ: zookeeperConfig.config.DB_HOST_READ,
  DB_HOST_WRITE: zookeeperConfig.config.DB_HOST_WRITE,
  DB_DATABASE: zookeeperConfig.config.DB_NAME,
  DB_PASSWORD: zookeeperConfig.config.DB_PASSWORD,
  DB_PORT: zookeeperConfig.config.DB_PORT,//
  BTC_RPC_URL: zookeeperConfig.config.BTC_RPC_URL,
  BTC_API_KEY: zookeeperConfig.config.BTC_API_KEY,
  // DEPOSIT_WITHDRAW_PROCESS_BTC: zookeeperConfig.config.DEPOSIT_WITHDRAW_PROCESS_BTC,
  // PENDING_WITHDRAWAL_TX_PROCESS_BTC: zookeeperConfig.config.PENDING_WITHDRAWAL_TX_PROCESS_BTC,
  // RABBIT_MQ: zookeeperConfig.config.RABBIT_MQ,
  REDIS_CONN: zookeeperConfig.config.REDIS_CONN,
  KAFKA_BROKER: zookeeperConfig.config.KAFKA_BROKER,
  KAFKA_CLIENT_ID: zookeeperConfig.config.KAFKA_CLIENT_ID,
  // BTC_ADDRESSES_PENDING_BALANCE_UPDATE: zookeeperConfig.config.BTC_ADDRESSES_PENDING_BALANCE_UPDATE,
  /**
   * KAFKA KEYS START
   */
  KAFKA_TRANSACTIONS: {
    DEPOSIT_WITHDRAW_GROUP_ID: zookeeperConfig.config.BTC_DEPOSIT_WITHDRAW_GROUP_ID,
    DEPOSIT_WITHDRAW_TOPIC: zookeeperConfig.config.BTC_DEPOSIT_WITHDRAW_TOPIC,
    BTC_PENDING_WITHDRAWAL_TX_TOPIC: zookeeperConfig.config.BTC_PENDING_WITHDRAWAL_TX_TOPIC,
    BTC_PENDING_WITHDRAWAL_TX_GROUP_ID: zookeeperConfig.config.BTC_PENDING_WITHDRAWAL_TX_GROUP_ID
  },
  KAFKA_BALANCE: {
    BTC_PENDING_BALANCE_UPDATE_TOPIC: zookeeperConfig.config.BTC_PENDING_BALANCE_UPDATE_TOPIC,
    BTC_PENDING_BALANCE_UPDATE_GROUP_ID: zookeeperConfig.config.BTC_PENDING_BALANCE_UPDATE_GROUP_ID
  },
  // REDIS_HOST_WRITE: zookeeperConfig.config.REDIS_HOST_WRITE,
  // REDIS_HOST_READ: zookeeperConfig.config.REDIS_HOST_READ,
  // REDIS_PORT: zookeeperConfig.config.REDIS_PORT,
  MIN_BLOCK_CONFORMATIONS: zookeeperConfig.config.MIN_BTC_BLOCK_CONFORMATIONS,
  MIN_BTC_TRANSACTION_CONFORMATIONS: zookeeperConfig.config.MIN_BTC_TRANSACTION_CONFORMATIONS,
  LAST_BLOCK_NUMBER_BTC: `${name}_${zookeeperConfig.config.SERVER}_${zookeeperConfig.config.LAST_BLOCK_NUMBER_BTC}`,
  TOKEN_TYPE_BTC: `${name}_${zookeeperConfig.config.SERVER}_${zookeeperConfig.config.TOKEN_TYPE_BTC}`,
  STATIC_COIN_FAMILY: zookeeperConfig.config.STATIC_COIN_FAMILY,
  UPDATE_BTC_BLOCK_STATUS: zookeeperConfig.config.UPDATE_BTC_BLOCK_STATUS,
  PREVIOUS_LAST_BLOCK_NUMBER: '',
  BTC_WALLET_ADDRESSES: `${name}_${zookeeperConfig.config.SERVER}_${zookeeperConfig.config.BTC_WALLET_ADDRESS}`,
  SERVER: zookeeperConfig.config.SERVER,

  REDISKEYS: {
    ALL_PROCESSES: zookeeperConfig.config.ALL_PROCESSES,
    SPECIFIC_BLOCK_BTC: zookeeperConfig.config.SPECIFIC_BLOCK_BTC
  },
  // added new
  KEYS: {
    FCM_PUSH: zookeeperConfig.config.FCM_PUSH
  },

}
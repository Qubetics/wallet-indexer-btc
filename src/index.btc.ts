import Zookeeper from './helpers/zookeeper.helper';
(async () => {
  await Zookeeper.connectZookeeper();
  await require('./config/index');
  await require('./server.btc');
})();
from EdithCloudNacos import NacosClient
from redis import Redis


class RedisConnectionConfigureFromNacosConfig:
    def __init__(self, nacos_client: NacosClient, config_name='redis'):
        self.config = nacos_client.get_config_from_data_id(config_name)
        self.host = self.config['spring']['redis']['host']
        self.port = self.config['spring']['redis']['port']
        self.password = self.config['spring']['redis']['password']
        self.database = self.config['spring']['redis']['database']


class RedisTemplate:
    def __init__(self, r: RedisConnectionConfigureFromNacosConfig):
        self.conn = Redis(host=r.host, port=r.port,
                          password=r.password, db=r.database,
                          decode_responses=True)

    def put(self, key: str, value: str, px):
        return self.conn.set(key, value, px=px)

    def __getitem__(self, item):
        return self.conn.get(item)



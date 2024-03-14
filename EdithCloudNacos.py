from hashlib import md5
from os.path import exists
from threading import Thread
from typing import Callable, Tuple

import requests
import yaml
import re

SUPPORTED_EXTENSION = ['yaml']
PROPERTIES_PATH = "static/properties.yml"
WORD_SEPARATOR = u'\x02'
LINE_SEPARATOR = u'\x01'
PULLING_TIMEOUT = 30 * 1000
CHARACTER = 'utf-8'


def dictToHttpRequestArgsStr(**kwargs):
    return '&'.join([f'{k}={v}' for k, v in kwargs.items()])


def composeHttpSentence(server_addr: str, router: str):
    return f'http://{server_addr}/{router}'


def composeHttpsSentence(server_addr: str, router: str):
    return f'https://{server_addr}/{router}'


def tokenizeHttpParams(s: str):
    params = {}
    sens = s.split('&')
    for sen in sens:
        k, v = sen.split('=')
        params[k] = v
    return params


class ImportConfigError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class ImportConfig:
    def __init__(self, data_id: str, namespace: str, extension: str, group='DEFAULT_GROUP'):
        self.dataId = data_id
        self.group = group
        self.namespace = namespace
        self.extension = extension

    def __eq__(self, other):
        return self.dataId == other.dataId and self.group == other.group and self.namespace == other.namespace


class ConfigCache:
    def __init__(self, import_config: ImportConfig, config=None):
        self.config = config
        self.feedback_functions = {}
        self.id = '%02'.join([import_config.dataId,
                              import_config.group])

    def add_feedback(self, name: str, f: Callable):
        self.feedback_functions[name] = f

    def del_feedback(self, name: str):
        self.feedback_functions.pop(name)

    def poll(self, key):
        for _, f in self.feedback_functions.items():
            f(key)


class NacosListener:
    def __init__(self, server_addr: str):
        self.server_addr = server_addr
        self.thread = None
        self.status = False
        self.caches = {}

    def put(self, import_config: ImportConfig, config_cache: ConfigCache):
        self.caches[config_cache.id] = (import_config, config_cache)

    def __tryResponse(self):
        while True:
            if not self.status:
                return
            id_list = []
            for _, (import_config, config_cache) in self.caches.items():
                key = WORD_SEPARATOR.join([f'{import_config.dataId}', import_config.group,
                                           md5(config_cache.config.encode(CHARACTER)).hexdigest()
                                           if config_cache.config else ''])
                id_list.append(key)
            self.__res = requests.post(url=composeHttpSentence(self.server_addr, 'nacos/v1/cs/configs/listener'),
                                       data={'Listening-Configs': LINE_SEPARATOR.join(id_list) + LINE_SEPARATOR},
                                       headers={'Long-Pulling-Timeout': str(PULLING_TIMEOUT)})
            if not self.__res.status_code == 200:
                return self.__res
            if self.__res.text:
                lines = self.__res.text.split('%01\n')
                for line in lines:
                    if line:
                        this = self.caches[line]
                        this[1].poll(this)

    def active(self):
        if not self.status:
            self.status = True
            self.thread = Thread(target=self.__tryResponse)
            self.thread.start()

    def terminate(self):
        self.status = False


class NacosClient:
    def __init__(self, properties_path=PROPERTIES_PATH):
        self.listener = None
        self.listenerHttpConn = None
        if not exists(properties_path):
            raise FileNotFoundError(f'{properties_path} not found')

        properties = yaml.load(open(properties_path), Loader=yaml.FullLoader)

        self.name = properties['spring']['application']['name']
        self.config_import = properties['spring']['config']['import']
        self.serverAddr = properties['spring']['cloud']['nacos']['server-addr']
        self.nacos_username = properties['spring']['cloud']['nacos']['username']
        self.nacos_password = properties['spring']['cloud']['nacos']['password']
        self.fileExtension = properties['spring']['cloud']['nacos']['config']['file-extension']
        self.namespace = properties['spring']['cloud']['nacos']['config']['namespace']

        if self.fileExtension not in SUPPORTED_EXTENSION:
            raise ValueError('Unsupported file extension')

        self.import_configs = []
        for imp in self.config_import:
            temp = imp.split(':')[1].split('?')
            dataId = temp[0]
            params = tokenizeHttpParams(temp[1])
            group = params['group']
            self.import_configs.append(ImportConfig(dataId, self.namespace, self.fileExtension, group))

        self.config_caches = []
        self.config_caches_mapping = {}
        for import_config in self.import_configs:
            cc = ConfigCache(import_config, self.get_config_raw(import_config))
            self.config_caches.append(cc)
            cc.add_feedback('', self.__default_feedback_function)
            self.config_caches_mapping[cc.id] = cc

        self.caches = [(self.import_configs[i], self.config_caches[i]) for i in range(len(self.import_configs))]

    def get_config_raw(self, import_config: ImportConfig):
        args = dictToHttpRequestArgsStr(tenant=import_config.namespace,
                                        dataId=import_config.dataId,
                                        group=import_config.group)
        response = requests.get(composeHttpSentence(self.serverAddr, f'nacos/v1/cs/configs?' + args))
        if response.status_code != 200:
            result = f'Cannot request config from Nacos(serverAddr={self.serverAddr})'
            raise ConnectionError(result + response.text)
        return response.text

    def active_config_listener(self):
        if not self.listener:
            self.listener = NacosListener(self.serverAddr)
            for ic, cc in self.caches:
                self.listener.put(ic, cc)

        self.listener.active()

    def get_config(self, index):
        if not 0 <= index < len(self.caches):
            raise IndexError(f'Not found config of the index {index}')
        ic, cc = self.caches[index]
        if ic.extension == 'yaml':
            return yaml.load(cc.config, Loader=yaml.FullLoader)

    def get_config_from_data_id(self, data_id: str):
        c = self.match_stand_config(data_id)
        for ic, cc in self.caches:
            if ic == c:
                if ic.extension == 'yaml':
                    return yaml.load(cc.config, Loader=yaml.FullLoader)

    def match_config(self, name):
        result = []
        for ic in self.import_configs:
            if re.match(rf'.*{name}.*', ic.dataId) or re.match(rf'.*{name}.*', ic.group):
                result.append(ic)
        return result

    def match_stand_config(self, config_name: str):
        configsMatched = self.match_config(config_name)
        if len(configsMatched) < 1:
            raise ImportConfigError(f'Importation Configuration like {config_name} Not Found')
        if len(configsMatched) > 1:
            raise ImportConfigError(f'Duplicated Importation Configuration like {config_name}')
        return configsMatched[0]

    def registerService(self, ip, port, service_name):
        return self.registerInstance(ip=ip, port=port, serviceName=service_name)

    def registerInstance(self, **kwargs):
        """
        :param kwargs:
        *ip, *port, *serviceName, namespaceId, weight, enabled,
         healthy, metadata, clusterName, groupName, ephemeral
        :return:
        """
        return requests.post(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/instance'),
                             data=kwargs)

    def __default_feedback_function(self, cc: Tuple[ImportConfig, ConfigCache]):
        cc[1].config = self.get_config_raw(cc[0])

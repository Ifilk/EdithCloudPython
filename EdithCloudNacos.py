import json
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
    data = []
    for k, v in kwargs.items():
        if v is None:
            continue
        data.append(f'{k}={v}')
    if len(data) == 0:
        return ''
    return '?' + '&'.join(data)


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

    def registerInstance(self, serviceName: str, ip: str, port: int, weight=None, enable=None, healthy=None,
                         metadata=None, groupName=None, clusterName=None, namespaceId=None, ephemeral=None):
        return requests.post(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/instance'),
                             data={ip: ip, port: port, namespaceId: namespaceId, weight: weight, enable: enable,
                                   healthy: healthy, metadata: metadata, clusterName: clusterName,
                                   serviceName: serviceName, groupName: groupName, ephemeral: ephemeral}).text

    def publishConfig(self, data_id: str, content: str, type: str, group='DEFAULT_GROUP'):
        if self.fileExtension not in SUPPORTED_EXTENSION:
            raise ValueError('Unsupported file extension')
        return bool(requests.post(url=composeHttpSentence(self.serverAddr, 'nacos/v1/cs/configs'),
                                  data={'dataId': data_id, 'group': group, 'content': content, 'type': type}).text)

    def deleteConfig(self, data_id: str, group='DEFAULT_GROUP'):
        return bool(requests.delete(url=composeHttpSentence(self.serverAddr, 'nacos/v1/cs/configs'),
                                    data={'dataId': data_id, 'group': group}).text)

    def getConfigHistory(self, dataId, group='DEFAULT_GROUP', tenant=None, pageNo=None, pageSize=None):
        """
        {
          "totalCount": 1,
          "pageNumber": 1,
          "pagesAvailable": 1,
          "pageItems": [
            {
              "id": "203",
              "lastId": -1,
              "dataId": "nacos.example",
              "group": "com.alibaba.nacos",
              "tenant": "",
              "appName": "",
              "md5": null,
              "content": null,
              "srcIp": "0:0:0:0:0:0:0:1",
              "srcUser": null,
              "opType": "I         ",
              "createdTime": "2010-05-04T16:00:00.000+0000",
              "lastModifiedTime": "2020-12-05T01:48:03.380+0000"
            }
          ]
        }
        """
        return json.loads(requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/cs/history' +
                                                               dictToHttpRequestArgsStr(search='accurate',
                                                                                        tenant=tenant,
                                                                                        dataId=dataId,
                                                                                        group=group,
                                                                                        pageNo=pageNo,
                                                                                        pageSize=pageSize))).text)

    def queryConfigHistory(self, nid: int, dataId: str, group: str, tenant=None):
        """
        {
          "id": "203",
          "lastId": -1,
          "dataId": "nacos.example",
          "group": "com.alibaba.nacos",
          "tenant": "",
          "appName": "",
          "md5": "9f67e6977b100e00cab385a75597db58",
          "content": "contentTest",
          "srcIp": "0:0:0:0:0:0:0:1",
          "srcUser": null,
          "opType": "I         ",
          "createdTime": "2010-05-04T16:00:00.000+0000",
          "lastModifiedTime": "2020-12-05T01:48:03.380+0000"
        }
        """
        return json.loads(requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/cs/history' +
                                                               dictToHttpRequestArgsStr(nid=nid,
                                                                                        tenant=tenant,
                                                                                        dataId=dataId,
                                                                                        group=group))).text)

    def queryPreviousConfigHistory(self, id: int, dataId: str, group: str, tenant=None):
        """
        {
          "id": "203",
          "lastId": -1,
          "dataId": "nacos.example",
          "group": "com.alibaba.nacos",
          "tenant": "",
          "appName": "",
          "md5": "9f67e6977b100e00cab385a75597db58",
          "content": "contentTest",
          "srcIp": "0:0:0:0:0:0:0:1",
          "srcUser": null,
          "opType": "I         ",
          "createdTime": "2010-05-04T16:00:00.000+0000",
          "lastModifiedTime": "2020-12-05T01:48:03.380+0000"
        }
        """
        return json.loads(requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/cs/history/previous' +
                                                               dictToHttpRequestArgsStr(id=id,
                                                                                        tenant=tenant,
                                                                                        dataId=dataId,
                                                                                        group=group))).text)

    def deleteInstance(self, serviceName: str, ip: str, port: int,
                       groupName=None, clusterName=None, namespaceId=None, ephemeral=None):
        return requests.delete(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/instance' +
                                                       dictToHttpRequestArgsStr(**{serviceName: serviceName,
                                                                                   groupName: groupName, ip: ip,
                                                                                   port: port,
                                                                                   clusterName: clusterName,
                                                                                   namespaceId: namespaceId,
                                                                                   ephemeral: ephemeral}))).text

    def updateInstance(self, serviceName: str, ip: str, port: int, weight=None, enable=None, healthy=None,
                       metadata=None, groupName=None, clusterName=None, namespaceId=None, ephemeral=None):
        return requests.put(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/instance' +
                                                    dictToHttpRequestArgsStr(**{ip: ip, port: port,
                                                                                namespaceId: namespaceId,
                                                                                weight: weight, enable: enable,
                                                                                healthy: healthy, metadata: metadata,
                                                                                clusterName: clusterName,
                                                                                serviceName: serviceName,
                                                                                groupName: groupName,
                                                                                ephemeral: ephemeral}))).text

    def queryInstanceList(self, serviceName: str, groupName=None, namespaceId=None, clusters=None, healthyOnly=None):
        """
        {
          "name": "DEFAULT_GROUP@@nacos.test.1",
          "groupName": "DEFAULT_GROUP",
          "clusters": "",
          "cacheMillis": 10000,
          "hosts": [
            {
              "instanceId": "10.10.10.10#8888#DEFAULT#DEFAULT_GROUP@@nacos.test.1",
              "ip": "10.10.10.10",
              "port": 8888,
              "weight": 1,
              "healthy": false,
              "enabled": true,
              "ephemeral": false,
              "clusterName": "DEFAULT",
              "serviceName": "DEFAULT_GROUP@@nacos.test.1",
              "metadata": { },
              "instanceHeartBeatInterval": 5000,
              "instanceIdGenerator": "simple",
              "instanceHeartBeatTimeOut": 15000,
              "ipDeleteTimeout": 30000
            }
          ],
          "lastRefTime": 1528787794594,
          "checksum": "",
          "allIPs": false,
          "reachProtectionThreshold": false,
          "valid": true
        }
        """
        return json.loads(requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/instance/list' +
                                                               dictToHttpRequestArgsStr(serviceName=serviceName,
                                                                                        groupName=groupName,
                                                                                        namespaceId=namespaceId,
                                                                                        clusters=clusters,
                                                                                        healthyOnly=healthyOnly)
                                                               )).text)

    def queryInstance(self, serviceName: str, ip: str, port: str, groupName=None, namespaceId=None,
                      clusters=None, healthyOnly=None):
        """
        {
            "metadata": {},
            "instanceId": "10.10.10.10-8888-DEFAULT-nacos.test.2",
            "port": 8888,
            "service": "nacos.test.2",
            "healthy": false,
            "ip": "10.10.10.10",
            "clusterName": "DEFAULT",
            "weight": 1.0
        }
        """
        return json.loads(requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/instance' +
                                                               dictToHttpRequestArgsStr(serviceName=serviceName,
                                                                                        groupName=groupName,
                                                                                        ip=ip,
                                                                                        port=port,
                                                                                        namespaceId=namespaceId,
                                                                                        clusters=clusters,
                                                                                        healthyOnly=healthyOnly)
                                                               )).text)

    def beatInstance(self, serviceName: str, ip: str, port: str, beat, namespaceId=None, groupName=None,
                     ephemeral=None):
        if beat is not str:
            beat = json.dumps(beat)
        return requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/instance/beat' +
                                                    dictToHttpRequestArgsStr(serviceName=serviceName,
                                                                             ip=ip,
                                                                             port=port,
                                                                             namespaceId=namespaceId,
                                                                             groupName=groupName,
                                                                             ephemeral=ephemeral,
                                                                             beat=beat)
                                                    )).text

    def createService(self, serviceName: str, groupName=None, namespaceId=None,
                      protectThreshold=None, metadata=None, selector=None):
        if not 0 <= protectThreshold <= 1:
            raise ValueError('ProtectThreshold must be between 0 and 1')
        if selector is not str:
            selector = json.dumps(selector)
        return requests.post(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/service'),
                             data={
                                 serviceName: serviceName,
                                 groupName: groupName,
                                 namespaceId: namespaceId,
                                 protectThreshold: protectThreshold,
                                 metadata: metadata,
                                 selector: selector
                             })

    def deleteService(self, serviceName: str, groupName=None, namespaceId=None):
        return requests.delete(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/service' +
                                                       dictToHttpRequestArgsStr(serviceName=serviceName,
                                                                                groupName=groupName,
                                                                                namespaceId=namespaceId))).text

    def updateService(self, serviceName: str, groupName=None, namespaceId=None,
                      protectThreshold=None, metadata=None, selector=None):
        if not 0 <= protectThreshold <= 1:
            raise ValueError('ProtectThreshold must be between 0 and 1')
        if selector is not str:
            selector = json.dumps(selector)
        return requests.put(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/service'),
                            data={
                                serviceName: serviceName,
                                groupName: groupName,
                                namespaceId: namespaceId,
                                protectThreshold: protectThreshold,
                                metadata: metadata,
                                selector: selector
                            })

    def queryService(self, serviceName: str, groupName=None, namespaceId=None):
        """
        {
            metadata: { },
            groupName: "DEFAULT_GROUP",
            namespaceId: "public",
            name: "nacos.test.2",
            selector: {
                type: "none"
            },
            protectThreshold: 0,
            clusters: [
                {
                    healthChecker: {
                        type: "TCP"
                    },
                    metadata: { },
                    name: "c1"
                }
            ]
        }
        :param serviceName:
        :param groupName:
        :param namespaceId:
        :return:
        """
        return json.loads(requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/service' +
                                                               dictToHttpRequestArgsStr(serviceName=serviceName,
                                                                                        groupName=groupName,
                                                                                        namespaceId=namespaceId))).text)

    def queryServiceList(self, pageNo: int, pageSize: int, groupName=None, namespaceId=None):
        """
        {
            "count":148,
            "doms": [
                "nacos.test.1",
                "nacos.test.2"
            ]
        }
        :param pageNo:
        :param pageSize:
        :param groupName:
        :param namespaceId:
        :return:
        """
        return json.loads(requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/service/list' +
                                                               dictToHttpRequestArgsStr(pageNo=pageNo,
                                                                                        pageSize=pageSize,
                                                                                        groupName=groupName,
                                                                                        namespaceId=namespaceId))).text)

    def querySwitch(self):
        """
        {
            name: "00-00---000-NACOS_SWITCH_DOMAIN-000---00-00",
            masters: null,
            adWeightMap: { },
            defaultPushCacheMillis: 10000,
            clientBeatInterval: 5000,
            defaultCacheMillis: 3000,
            distroThreshold: 0.7,
            healthCheckEnabled: true,
            distroEnabled: true,
            enableStandalone: true,
            pushEnabled: true,
            checkTimes: 3,
            httpHealthParams: {
                max: 5000,
                min: 500,
                factor: 0.85
            },
            tcpHealthParams: {
                max: 5000,
                min: 1000,
                factor: 0.75
            },
            mysqlHealthParams: {
                max: 3000,
                min: 2000,
                factor: 0.65
            },
            incrementalList: [ ],
            serverStatusSynchronizationPeriodMillis: 15000,
            serviceStatusSynchronizationPeriodMillis: 5000,
            disableAddIP: false,
            sendBeatOnly: false,
            limitedUrlMap: { },
            distroServerExpiredMillis: 30000,
            pushGoVersion: "0.1.0",
            pushJavaVersion: "0.1.0",
            pushPythonVersion: "0.4.3",
            pushCVersion: "1.0.12",
            enableAuthentication: false,
            overriddenServerStatus: "UP",
            defaultInstanceEphemeral: true,
            healthCheckWhiteList: [ ],
            checksum: null
        }
        :return:
        """
        return json.loads(requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/operator/switches')).text)

    def updateSwitch(self, entry: str, value: str, debug=None):
        return requests.put(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/operator/switches' +
                                                    dictToHttpRequestArgsStr(entry=entry,
                                                                             value=value,
                                                                             debug=debug))).text

    def queryMetrics(self):
        """
        {
            serviceCount: 336,
            load: 0.09,
            mem: 0.46210432,
            responsibleServiceCount: 98,
            instanceCount: 4,
            cpu: 0.010242796,
            status: "UP",
            responsibleInstanceCount: 0
        }
        :return:
        """
        return json.loads(requests.get(composeHttpSentence(self.serverAddr, 'nacos/v1/ns/operator/metrics')).text)

    def queryServerList(self, healthy=None):
        """
        {
            servers: [
                {
                    ip: "1.1.1.1",
                    servePort: 8848,
                    site: "unknown",
                    weight: 1,
                    adWeight: 0,
                    alive: false,
                    lastRefTime: 0,
                    lastRefTimeStr: null,
                    key: "1.1.1.1:8848"
                },
                {
                    ip: "1.1.1.2",
                    servePort: 8848,
                    site: "unknown",
                    weight: 1,
                    adWeight: 0,
                    alive: false,
                    lastRefTime: 0,
                    lastRefTimeStr: null,
                    key: "1.1.1.2:8848"
                },
                {
                    ip: "1.1.1.3",
                    servePort: 8848,
                    site: "unknown",
                    weight: 1,
                    adWeight: 0,
                    alive: false,
                    lastRefTime: 0,
                    lastRefTimeStr: null,
                    key: "1.1.1.3:8848"
                }
            ]
        }
        :param healthy:
        :return:
        """
        return json.loads(requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/operator/servers' +
                                                               dictToHttpRequestArgsStr(healthy=healthy))).text)

    def queryLeader(self):
        """
        {
            leader: "{"heartbeatDueMs":2500,"ip":"1.1.1.1:8848","leaderDueMs":12853,"state":"LEADER","term":54202,"voteFor":"1.1.1.1:8848"}"
        }
        :return:
        """
        return json.loads(requests.get(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/raft/leader')).text)

    def updateInstanceHealthy(self, serviceName: str, ip: str, port: int, healthy: bool,
                              namespaceId=None, groupName=None, clusterName=None):
        return requests.put(requests.put(url=composeHttpSentence(self.serverAddr, 'nacos/v1/ns/health/instance' +
                                                                 dictToHttpRequestArgsStr(namespaceId=namespaceId,
                                                                                          serviceName=serviceName,
                                                                                          groupName=groupName,
                                                                                          clusterName=clusterName,
                                                                                          ip=ip, port=port,
                                                                                          healthy=healthy))).text)

    def __default_feedback_function(self, cc: Tuple[ImportConfig, ConfigCache]):
        cc[1].config = self.get_config_raw(cc[0])

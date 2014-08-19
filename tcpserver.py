#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
    面向指令透传的TCPServer
    1、device_id的组成方式为ip_port
    2、设备类型为0，未知
    3、通过全局变量来管理每个介入设备，信息包括device_id，thread对象，handler对象
"""
import sys
import json
from SocketServer import ThreadingTCPServer, StreamRequestHandler
import paho.mqtt.client as mqtt
import threading
import traceback
import logging
import ConfigParser
import binascii
try:
    import paho.mqtt.publish as publish
except ImportError:
    # This part is only required to run the example from within the examples
    # directory when the module itself is not installed.
    #
    # If you have the module installed, just use "import paho.mqtt.publish"
    import os
    import inspect
    cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"../src")))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)
    import paho.mqtt.publish as publish

from json import loads, dumps

from libs.utils import *

# 设置系统为utf-8  勿删除
reload(sys)
sys.setdefaultencoding('utf-8')

# 全局变量
# 设备信息字典
devices_info_dict = dict()
thread_dict = dict()

# 切换工作目录
# 程序运行路径
procedure_path = cur_file_dir()
# 工作目录修改为python脚本所在地址，后续成为守护进程后会被修改为'/'
os.chdir(procedure_path)

# 日志对象
logger = logging.getLogger('tcpserver')
hdlr = logging.FileHandler('./tcpserver.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.WARNING)

# 加载配置项
config = ConfigParser.ConfigParser()
config.read("./tcpserver.cfg")
tcp_server_ip = config.get('server', 'ip')
tcp_server_port = int(config.get('server', 'port'))
mqtt_server_ip = config.get('mqtt', 'server')
mqtt_server_port = int(config.get('mqtt', 'port'))
gateway_topic = config.get('gateway', 'topic')
device_network = config.get('device', 'network')
data_protocol = config.get('device', 'protocol')

# 获取本机ip
ip_addr = get_ip_addr()

# 加载设备信息字典
devices_info_file = "devices.txt"

# 新增设备
def check_device(device_id, device_type, device_addr, device_port):
    # 如果设备不存在则设备字典新增设备并写文件
    if device_id not in devices_info_dict:
        # 新增设备到字典中
        devices_info_dict[device_id] = {
            "device_id": device_id,
            "device_type": device_type,
            "device_addr": device_addr,
            "device_port": device_port
        }
        logger.debug("发现新设备%r" % devices_info_dict[device_id])
        #写文件
        devices_file = open(devices_info_file, "w+")
        devices_file.write(dumps(devices_info_dict))
        devices_file.close()


def publish_device_data(device_id, device_type, device_addr, device_port, device_data):
    # device_data: 16进制字符串
    # 组包
    device_msg = {
        "device_id": device_id,
        "device_type": device_type,
        "device_addr": device_addr,
        "device_port": device_port,
        "protocol": data_protocol,
        "data": device_data
    }

    # MQTT发布
    publish.single(topic=gateway_topic,
                   payload=json.dumps(device_msg),
                   hostname=mqtt_server_ip,
                   port=mqtt_server_port)
    logger.info("向Topic(%s)发布消息：%s" % (gateway_topic, device_msg))


# 串口数据读取线程
def process_mqtt(device_id, handler):
    """
    :param data_sender: 业务数据队列
    :return:
    """
    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, rc):
        logger.info("Connected with result code " + str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        mqtt_client.subscribe(device_id)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        logger.info("收到数据消息" + msg.topic + " " + str(msg.payload))
        # 消息只包含device_cmd，16进制字符串
        device_cmd = json.loads(msg.payload)["command"]
        handler.wfile.write(binascii.a2b_hex(device_cmd))
        logger.info("向地址(%r)发送数据%s" % (handler.client_address, device_cmd))

    mqtt_client = mqtt.Client(client_id=device_id)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    try:
        mqtt_client.connect(mqtt_server_ip, mqtt_server_port, 60)

        # Blocking call that processes network traffic, dispatches callbacks and
        # handles reconnecting.
        # Other loop*() functions are available that give a threaded interface and a
        # manual interface.
        mqtt_client.loop_forever()
    except Exception, e:
        logger.error("MQTT链接失败，错误内容:%r" % e)


class MyStreamRequestHandlerr(StreamRequestHandler):
    """
    #StreamRequestHandler，并重写handle方法
    #（StreamRequestHandler继承自BaseRequestHandler）
    """
    def handle(self):
        # 绑定设备信息
        device_id = "%s/%s/%s" % (device_network, self.client_address[0], self.client_address[1])
        check_device(device_id, 0, self.client_address[0], self.client_address[1])

        # 创建MQTT监控线程
        process_thread = threading.Thread(target=process_mqtt, args=(device_id, self))
        process_thread.start()
        thread_dict[device_id] = {"thread": process_thread, "handler": self}
        device_info = devices_info_dict[device_id]
        while True:
            #客户端主动断开连接时，self.rfile.readline()会抛出异常
            try:
                #self.rfile类型是socket._fileobject,读写模式是"rb",方法有
                #read,readline,readlines,write(data),writelines(list),close,flush
                data = self.rfile.readline().strip()
                #self.client_address是客户端的连接(host, port)的元组
                logger.info("receive from (%r):%r" % (self.client_address, data))
                hex_data = binascii.b2a_hex(data)
                publish_device_data(device_info["device_id"],
                                    device_info["device_type"],
                                    device_info["device_addr"],
                                    device_info["device_port"],
                                    hex_data)
                logger.info("向Topic(%s)发布消息：%r" % (gateway_topic, hex_data))
            except:
                traceback.print_exc()
                break

        # 退出时，清除对应字典项
        del thread_dict[device_id]


if __name__ == "__main__":
    addr = (tcp_server_ip, tcp_server_port)

    #ThreadingTCPServer从ThreadingMixIn和TCPServer继承
    #class ThreadingTCPServer(ThreadingMixIn, TCPServer): pass
    server = ThreadingTCPServer(addr, MyStreamRequestHandlerr)

    #启动服务监听
    server.serve_forever()




#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
    面向指令透传的TCPServer
    1、device_id的组成方式为ip_port
    2、设备类型为0，未知
    3、通过全局变量来管理每个介入设备，信息包括device_id，thread对象，handler对象
"""
import sys
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


# 全局变量
devices_dict = dict()

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
mqtt_server_ip = config.get('mqtt', 'ip')
mqtt_server_port = int(config.get('mqtt', 'port'))
gateway_topic= config.get('gateway', 'topic')


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
        client.subscribe(device_id)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        logger.info("收到数据消息" + msg.topic + " " + str(msg.payload))
        # 消息只包含device_cmd，16进制字符串
        device_cmd = msg.payload
        handler.wfile.write(binascii.a2b_hex(device_cmd))
        logger.info("向地址(%r)发送数据%s" % (handler.client_address, device_cmd))

    client = mqtt.Client(client_id="gateway")
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(mqtt_server_ip, mqtt_server_port, 60)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    client.loop_forever()


class MyStreamRequestHandlerr(StreamRequestHandler):
    """
    #StreamRequestHandler，并重写handle方法
    #（StreamRequestHandler继承自BaseRequestHandler）
    """
    def handle(self):
        # 绑定设备信息
        device_id = "%s_%d" % (self.client_address[0], self.client_address[1])
        # 创建MQTT监控线程
        process_thread = threading.Thread(target=process_mqtt, args=(device_id, self))
        process_thread.start()
        devices_dict[device_id] = {"thread": process_thread, "handler": self}

        while True:
            #客户端主动断开连接时，self.rfile.readline()会抛出异常
            try:
                #self.rfile类型是socket._fileobject,读写模式是"rb",方法有
                #read,readline,readlines,write(data),writelines(list),close,flush
                data = self.rfile.readline().strip()
                #self.client_address是客户端的连接(host, port)的元组
                logger.info( "receive from (%r):%r" % (self.client_address, data))

                # 消息编码
                device_msg = {
                    "device_id": device_id,
                    "device_type": 0,
                    "device_addr": self.client_address[0],
                    "device_port": self.client_address[1],
                    "device_data": data
                }

                # MQTT发布
                publish.single(topic=gateway_topic,
                               payload=device_msg,
                               hostname=mqtt_server_ip,
                               port=mqtt_server_port)
                logger.info("向Topic(%s)发布消息：%r" % (gateway_topic, device_msg))
            except:
                traceback.print_exc()
                break

        # 退出时，清除对应字典项
        del devices_dict[device_id]


if __name__ == "__main__":
    addr = (tcp_server_ip, tcp_server_port)

    #ThreadingTCPServer从ThreadingMixIn和TCPServer继承
    #class ThreadingTCPServer(ThreadingMixIn, TCPServer): pass
    server = ThreadingTCPServer(addr, MyStreamRequestHandlerr)

    #启动服务监听
    server.serve_forever()


    
    
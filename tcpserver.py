#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
    面向指令透传的TCPServer
    1、device_id的组成方式为ip_port
    2、设备类型为0，未知
    3、通过全局变量来管理
"""

from SocketServer import ThreadingTCPServer, StreamRequestHandler
import paho.mqtt.client as mqtt
import traceback

# 全局变量
devices_dict = dict()

# 串口数据读取线程
def process_mqtt(device_id):
    """
    :param data_sender: 业务数据队列
    :return:
    """
    # The callback for when the client receives a CONNACK response from the server.
    def on_connect(client, userdata, rc):
        interface_logger.info("Connected with result code " + str(rc))
        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe(appsettings.MQTT_TOPIC)

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        interface_logger.info("收到数据消息" + msg.topic + " " + str(msg.payload))
        # csv解码
        data_list = msg.split(',')
        if len(data_list) < 4:
            interface_logger.error("数据内容错误:%s" % msg)
        else:
            # 构造消息包
            data_msg = dict()
            data_msg["message_type"] = const.INTERNAL_MESSAGE_TYPE_SENSOR_DATA
            data_msg["device_id"] = data_list[0]
            data_msg["device_addr"] = data_list[1]
            data_msg["device_port"] = data_list[2]
            data_msg["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
            data_msg["device_type"] = 0
            data_msg["data"] = data_list[3]
            # 检查设备是否存在，如果不存在则创建设备并插入
            check_device(data_msg)
            # 发送数据到业务层
            data_sender.sender(data_msg)

    client = mqtt.Client(client_id="gateway")
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(appsettings.MQTT_SERVER, appsettings.MQTT_PORT, 60)

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
        # 创建MQTT监控线程
        while True:
            #客户端主动断开连接时，self.rfile.readline()会抛出异常
            try:
                #self.rfile类型是socket._fileobject,读写模式是"rb",方法有
                #read,readline,readlines,write(data),writelines(list),close,flush
                data = self.rfile.readline().strip()

                # 消息编码

                # MQTT发布
                
                #self.client_address是客户端的连接(host, port)的元组
                print "receive from (%r):%r" % (self.client_address, data)
                
                #self.wfile类型是socket._fileobject,读写模式是"wb"
                self.wfile.write(data.upper())
            except:
                traceback.print_exc()
                break

# 
def process_tcpserver(addr):
    #ThreadingTCPServer从ThreadingMixIn和TCPServer继承
    #class ThreadingTCPServer(ThreadingMixIn, TCPServer): pass
    server = ThreadingTCPServer(addr, MyStreamRequestHandlerr)
    
    #启动服务监听
    server.serve_forever()

if __name__ == "__main__":
    #telnet 127.0.0.1 9999
    host = ""      #主机名，可以是ip,像localhost的主机名,或""
    port = 9999    #端口
    addr = (host, port)

    # 处理线程
    mqtt_thread = threading.Thread(target=process_mqtt)
    mqtt_thread..start()

    tcpserver_thread = threading.Thread(target=process_tcpserver, args=(addr))
    tcpserver_thread.start()
    
    
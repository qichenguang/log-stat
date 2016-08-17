
# coding: utf-8

import sys
import time
import traceback
from collections import defaultdict

class Cache(object):

    def __init__(self, max_size=120):
        self.max_size = max_size
        self.counter_cache = defaultdict(lambda: defaultdict(lambda: 0))

    def incr(self, key, timestamp, value=1):
        self.counter_cache[timestamp][key] += value

    def is_full(self):
        if len(self.counter_cache) >= self.max_size:
            return True
        return False

    def flush(self):
        if not self.is_full():
            return None
        #
        cache = self.counter_cache
        msg = []
        for timestamp, key_values in cache.items():
            for key, value in key_values.items():
                msg.append("%s %s %s" % (key, str(value), str(timestamp)))
        #
        cache.clear()
        return "\n".join(msg) + "\n"

import os
from datetime import datetime

class DayFile(object):
    def __init__(self,base_path = "/data0/tmp"):
        self.base_path = base_path
        self.cur_day_str = None
        self.real_path = None
        self.fp = None

    def writeLine(self,msg):
        try:
            #cur_day_str = datetime.now.strftime('%Y%m%d')
            cur_day_str = time.strftime("%Y%m%d", time.localtime())
            if self.cur_day_str != cur_day_str:
                self.cur_day_str = cur_day_str
                #
                if self.fp:
                    self.fp.close()
                #
                self.real_path = os.path.realpath(self.base_path + "/" + cur_day_str + ".txt")
                self.fp = open(self.real_path,"a")
            #
            if self.fp:
                self.fp.write(msg)

        except IOError as error:
            traceback.print_exc()
        except OSError as error:
            traceback.print_exc()



import sys
from twisted.internet.protocol import ServerFactory
from twisted.protocols.basic import LineReceiver
from twisted.python import log
from twisted.internet import reactor

import socket
class LineProtocol(LineReceiver):

    #
    #delimiter = b'\r\n'
    #
    def write_graphite(self, msg):
        ip = '10.77.96.122'
        port = 2003
        try:
            self.graphite_sock.send(msg.encode("UTF-8"))
        except Exception:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, port))
            sock.send(msg.encode("UTF-8"))
            self.graphite_sock = sock

    def connectionMade(self):
        self.client_ip = self.transport.getPeer()
        log.msg("Client connection from %s" % self.client_ip)
        if len(self.factory.clients) >= self.factory.clients_max:
            log.msg("Too many connections. bye !")
            self.client_ip = None
            self.transport.loseConnection()
        else:
            self.factory.clients.append(self.client_ip)

    def connectionLost(self, reason):
        log.msg('Lost client connection.  Reason: %s' % reason)
        if self.client_ip:
            self.factory.clients.remove(self.client_ip)

    def lineReceived(self, line):
        log.msg('Cmd received from %s : %s' % (self.client_ip, line.decode()))
        try:
            interface,ret_time,timestamp= line.decode().split()
            qps_key_prev = 'test.sdkapp.access.qps.'
            self.factory.cache.incr(qps_key_prev + interface,timestamp)
            if self.factory.cache.is_full():
                msg = self.factory.cache.flush()
                if msg:
                    self.write_graphite(msg)

            #
            time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp)))
            self.factory.dayFile.writeLine("%s,%s,%s\n" % (time_str,interface.split("-")[-2],ret_time))
        except Exception:
            traceback.print_exc()



class LineFactory(ServerFactory):

    protocol = LineProtocol

    def __init__(self, clients_max=100):
        self.clients_max = clients_max
        self.clients = []
        self.cache = Cache(1)
        self.dayFile = DayFile("/data0/tmp")


log.startLogging(sys.stdout)
reactor.listenTCP(33333, LineFactory(1))
reactor.run()





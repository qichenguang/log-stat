# coding: utf-8

import optparse
import os
import sys
import time
import re
from datetime import datetime
import traceback

class Error(Exception):
    pass

class FileError(Error):
    traceback.print_exc()
    pass

class Tailor(object):
    def __init__(self,
                 base_path = "/data0/nginx/logs",
                 file_name = "sdkapp.mobile.sina.cn_access.log",
                 gen_file_type = "same-name",
                 begin_pos = "end",
                 sleep_sec = 1,
                 reopen_count = 5):
        self.base_path = base_path
        self.file_name = file_name
        self.gen_file_type = gen_file_type
        self.begin_pos = begin_pos
        self.sleep_sec = sleep_sec
        self.reopen_count = reopen_count
        self.inode = -1
        self.log_file = None

    def open_file(self):
        try:
            # gen real file name
            if self.gen_file_type == "same-name":
                self.real_path = os.path.realpath(self.base_path + "/" + self.file_name)
                self.inode = os.stat(self.real_path).st_ino
            elif self.gen_file_type == "every-day-dir":
                cur_day_str = datetime.now.strftime('%Y-%m-%d')
                self.real_path = os.path.realpath(self.base_path + "/" + cur_day_str + "/" + self.file_name)
                self.inode = os.stat(self.real_path).st_ino
            else:
                raise ValueError('invalid gen_file_type argument')

            # open file
            self.log_file = open(self.real_path)
            pos_dict = {"begin": 0, 'current': 1, 'end': 2}
            pos_flag = pos_dict[self.begin_pos]
            self.log_file.seek(0, pos_flag)

        except IOError as error:
            traceback.print_exc()
            raise FileError(error)
        except OSError as error:
            traceback.print_exc()
            raise FileError(error)

    def close_file(self):
        try:
            self.log_file.close()
        except Exception:
            pass

    def reopen(self):
        self.close_file()
        reopen_count = self.reopen_count
        while reopen_count >= 0:
            try:
                self.open_file()
                return True
            except FileError:
                time.sleep(self.sleep_sec)
            #
            reopen_count -= 1
        return False

    def __iter__(self):
        while True:
            pos = self.log_file.tell()
            line = self.log_file.readline()
            if not line:
                self.wait_line(pos)
            else:
                yield line

    def wait_line(self, pos):
        if self.check_switch_to_newfile(pos):
            if not self.reopen():
                time.sleep(self.sleep_sec)
        else:
            self.log_file.seek(pos)
            time.sleep(self.sleep_sec)

    def check_switch_to_newfile(self, pos):
        try:
            # change dir
            if self.gen_file_type == "every-day-dir":
                cur_day_str = time.strftime('%Y-%m-%d', time.localtime(time.time()))
                if self.real_path != os.path.realpath(self.base_path + "/" + cur_day_str + "/" + self.file_name):
                    return True
            # change new same name file
            stat = os.stat(self.real_path)
            if self.inode != stat.st_ino:
                return True
            # pos > file size
            if pos > stat.st_size:
                return True
        except OSError as oserror:
            # 出现错误,重新打开文件
            return True
        # 默认没有切换文件
        return False

class LogParser(object):

    def __init__(self, category):
        self.category = category
        self.operator = {'access' : self.__access,
                         'timeout': self.__timeout}
        do_process = self.operator.get(self.category, False)
        if not do_process:
            print('No such category: [access]|[timeout]')
            sys.exit(0)
        else:
            self.do_process = do_process

    def adjust(self, interface):
        interface = interface.replace('.', '-')
        interface = interface.replace('/', '-')
        interface = interface.lstrip('-')
        return interface

    def __access(self, line):
        try:
            result = line.split("|")
            if result != None and len(result) == 14:
                nowtime = result[2].lstrip('[').rstrip(']').split()[0]
                #
                timearray = time.strptime(nowtime, "%d/%b/%Y:%H:%M:%S")
                timestamp = int(time.mktime(timearray))
                #
                interface = result[3].replace("?", " ").split()[1]
                interface = self.adjust(interface)
                #
                ret_time = float(result[10].strip("\""))
                #
                return timestamp, interface, ret_time
        except Error as error:
            traceback.print_exc()

    def __timeout(self, line):
        pass


import socket

class ClientSocket(object):
    def __init__(self, host='127.0.0.1', port=33333):
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect()

    def connect(self):
        self.sock.connect((self.host, self.port))

    def reconnect(self):
        try:
            self.sock.close()
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connect()
        except Exception:
            pass

    def send(self, msg):
        try:
            self.sock.send(msg.encode("UTF-8"))
        except Exception as ex:
            print(ex)
            traceback.print_exc()
            self.reconnect()

def run():
    #
    log_parser = LogParser("access")
    #
    client = ClientSocket(host='172.16.193.114', port=33333)
    #
    tailor = Tailor(base_path="/data0/nginx/logs", file_name="sdkapp.mobile.sina.cn_access.log", gen_file_type="same-name", begin_pos="end")
    #
    #qps_key_prev = 'test.sdkapp.access.qps.'
    store = []
    try:
        tailor.open_file()
        for line in tailor:
            try:
                timestamp, interface, ret_time = log_parser.do_process(line)
                store.append("%s %s %s" % (str(interface), str(ret_time), str(timestamp)))
                if len(store) > 2:
                    client.send("\r\n".join(store) + "\r\n")
                    store.clear()
            except Exception:
                pass

    finally:
        tailor.close_file()


run()








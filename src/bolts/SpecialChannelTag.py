#coding=utf-8
import os
import time

class JudgmentChannel(object):

    @classmethod
    def OneTwoThree(cls, pid, business):
        if str(pid).startswith('sogou-navi-'):
            return 'pc_channel_123'
        else:
            return 'UNKNOW'


    @classmethod
    def BC(cls, pid, upos_asid):
        if str(pid).startswith('1') and (int(upos_asid) >> 30) & 1 == 1:
            return 'pc_channel_bc'
        else:
            return 'UNKNOW'


    @classmethod
    def BD(cls, pid, business):
        if str(business) in ['10300', '10301'] and str(pid) not in ['sogou', 'sogou-nopid', 'sogou-wrongpid']:
            return 'pc_channel_bd'
        else:
            return 'UNKNOW'

    @classmethod
    def BRWS(cls, pid, business):
        if (str(business) == '10400' and not str(pid).startswith('sogou-misc-85bcf11c65e51c2a')) or \
           (str(business) == '10401' and str(pid) not in ['sogou', 'sohu']):
            return 'pc_channel_brws'
        else:
            return 'UNKNOW'   

    @classmethod
    def IME(cls, pid, business):
        if str(business) == '10500' and \
         str(pid).find('sogou-clse-0dc94ddee6d0b5cf') != 0 and \
         str(pid).find('sogou-clse-eda5b489bc6d0b7d') != 0 and \
         str(pid).find('sogou-clse-f26a1be24a9ff7bb') != 0:
            return 'pc_channel_ime'
        else:
            return 'UNKNOW'

    @classmethod
    def SOSO(cls, business):
        if str(business) == '10103':
            return 'pc_channel_soso'
        else:
            return 'UNKNOW'

    @classmethod
    def SOGOU(cls, pid, business):
        if str(business) == '10100' or \
           str(pid) in ['sogou', 'sogou-nopid', 'sogou-wrongpid']:
            return 'pc_channel_sogou'
        else:
            return 'UNKNOW'

    @classmethod
    def SOHU(cls, pid, business):
        if str(business) == '10200' or str(pid) == 'sohu':
            return 'pc_channel_sohu'
        else:
            return 'UNKNOW'

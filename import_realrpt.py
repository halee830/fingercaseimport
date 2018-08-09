# -*- coding: utf-8 -*-
from email.parser import Parser
import email
import email.header
import poplib
import time
import base64,os
import xlrd
import sqlite3
import datetime
import cx_Oracle as db
import os,sys

global db_error
db_error = False


def code_convert(info):                       #邮件转码
    dh = email.header.decode_header(info)
    if dh[0][1] is None:
        convered = dh[0][0]
    else:
        convered = dh[0][0].decode(dh[0][1])
    return convered
def get_attachment(msg):                      #下载附件并返回完整文件名，如果没有附件返回None
    for par in msg.walk():
        if not par.is_multipart():
            name = par.get_param("name")
            if name:
                fname = code_convert(name)

                print('附件名:', fname)
                data = par.get_payload(decode=True)  # 解码出附件数据，然后存储到文件中
                try:
                    f = open(fname, 'wb')  # 注意一定要用wb来打开文件，因为附件一般都是二进制文件
                except:
                    print('附件名有非法字符，自动换一个aaaa.xls')
                    fname = 'D:/python/fingercaseimport/aaaa.xls'
                    f = open(fname, 'wb')
                f.write(data)
                f.close()
                return fname

            else:
                pass

    return None

def deal_mail(server,index):                   #读取单个邮件
    resp, lines, octets = server.retr(index)  # lines存储邮件的原始文本的每一行
    # 获得整个邮件的原始文本:
    msg_content = b'\r\n'.join(lines).decode('utf-8')

    # 解析出邮件: 把邮件文本解析成Message对象,还需要更多内容...
    msg = Parser().parsestr(msg_content)
    # print(msg)
    fromwho = code_convert(msg.get("from"))    #发件人

    print("发件人：", fromwho)

    maildate = datetime.datetime.strptime(msg.get("date"), '%a, %d %b %Y %H:%M:%S +0800')               #发件时间

    print("邮件时间：", maildate,"   ",type(maildate))

    subject = code_convert(msg.get("subject"))  # 取信件头里的subject,　也就是主题
    print("邮件主题:", subject)

    if '通融' in subject:
        fname = get_attachment(msg)
        if fname is None:
            print("通融邮件：", subject,"邮件时间：",maildate,"——获取附件失败")
            return
        if deal_tongrong(fname,fromwho,maildate) = 1:
            server.dele(index)

def deal_tongrong(fname,fromwho,maildate):
    #把记录放到表szcx.auto_claim_tongrong
    try:
        data = xlrd.open_workbook(fname)
    except Exception as e:
        print(str(e))
        print(fromwho,"---",maildate,"---",fname,"---","打开附件失败")
        return -1
    # 取第一个Sheet
    table = data.sheet_by_index(0)
    nrows = table.nrows

    if nrows > 0:
        global db_error
        if db_error:
            return -1
        try:
            con = db.connect('szcx/Ora_szcx@10.187.12.60:1522/pshz189')  # 连接数据库,没有写try...
        except db.DatabaseError as exc:
            error, = exc.args
            print("Oracle-Error-Code:", error.code, " Oracle-Error-Message:", error.message)
            db_error = True
            return -1
        for i in range(nrows):
            print(table.row_values(i),fromwho,maildate)
            print('111xxx')
        con.close()
        return 1
def main():

    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

    # os.chdir('/home/kettle/python/temp')
    os.chdir('D:/python/fingercaseimport')
    # today=time.strftime('%Y/%m/%d',time.localtime())
    # sub="Fwd: %s - D10 - Daily Details Report & Statement" % (today)
    # print("today:"+today)
    # print("sub:"+sub)

    # 邮件地址, 口令和POP3服务器地址:
    user = 'baoanshuju@cpic.com.cn'
    password = 'Cpic12345'
    pop3_server = '10.191.48.9'
    # 连接到POP3服务器:
    server = poplib.POP3(pop3_server)
    # server.set_debuglevel(1)  # 调试信息
    # 身份认证:
    server.user(user)
    server.pass_(password)

    # stat()返回(邮件数量,占用空间)
    # print('Messages: %s. Size: %s' % server.stat())
    # list()返回所有邮件的编号: (response, ['mesg_num octets', ...], octets)
    resp, mails, octets = server.list()
    # 查看返回的列表，类似  [b'1 4482', b'2 23956', ...]
    # print(mails)

    # 获取最新一封邮件, 注意索引号从1开始:
    index = len(mails)
    if index == 0:
        print('没有需要处理的邮件。', time.asctime(time.localtime(time.time())))
        exit()
    for i in range(1, index):
        deal_mail(server, index + 1 - i)

if __name__ == '__main__':
    main()
# -*- coding: utf-8 -*-
from email.parser import Parser
import email
import email.header
import poplib,smtplib
import time
import base64,os
import xlrd
import sqlite3
import datetime
import cx_Oracle as db
import os,sys
import configparser
from email.mime.text import MIMEText
from email.header import Header
import email.mime.multipart
import email.mime.text

global db_error
global db_info

db_error = False


def code_convert(info):                       #邮件转码
    dh = email.header.decode_header(info)
    if dh[0][1] is None:
        convered = dh[0][0]
    else:
        convered = dh[0][0].decode(dh[0][1])
        #print(dh[0][0],dh[0][1])
    return convered

def send_mail( rec_name, rec_address, subject, mailmsg ) :    #收件人名字，地址，标题，正文

    global user, password, pop3_server
    reply_message = MIMEText(mailmsg, 'html', 'utf-8')
    reply_message.add_header("Content-Type", 'text/plain; charset="utf-8"')
    reply_message['from'] = user
    reply_message['To'] = rec_name
    reply_message['Subject'] = 'Re:' + subject

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.command_encoding = 'utf-8'
        smtpObj.connect(pop3_server)
        smtpObj.login(user, password)
        smtpObj.sendmail(user, [rec_address], reply_message.as_string())
        print("邮件发送成功")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")

    smtpObj.quit()
    return

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

def deal_mail(server,index,conf):                   #读取单个邮件


    global user,password,pop3_server
    resp, lines, octets = server.retr(index)  # lines存储邮件的原始文本的每一行
    # 获得整个邮件的原始文本:
    msg_content = b'\r\n'.join(lines).decode('utf-8')

    # 解析出邮件: 把邮件文本解析成Message对象,还需要更多内容...
    msg = Parser().parsestr(msg_content)
    # print(msg)
    fromwho = code_convert(msg.get("from"))    #发件人
    if fromwho in conf['maillist']:
        receiver = conf['maillist'][fromwho]
    else:
        retrun
    #print("发件人：", fromwho)

    maildate = datetime.datetime.strptime(msg.get("date"), '%a, %d %b %Y %H:%M:%S +0800')               #发件时间

    #print("邮件时间：", maildate,"   ",type(maildate))

    subject = code_convert(msg.get("subject"))  # 取信件头里的subject,　也就是主题
    #print("邮件主题:", subject)

    if '通融' in subject:
        fname = get_attachment(msg)
        if fname is None:
            print("通融邮件：", subject,"邮件时间：",maildate,"——获取附件失败")
            return
        li_success,ls_dealmsg=deal_tongrong(fname,fromwho,maildate)
        if li_success== 1:
            server.noop()
            server.dele(index)
            ls_dealmsg = "邮件数据处理成功"+'\r\n'+ls_dealmsg
        else:
            ls_dealmsg = "邮件数据处理失败" + '\r\n' + ls_dealmsg
        send_mail(fromwho,receiver,subject,ls_dealmsg)    #收件人中文地址，收件人英文地址，标题，正文


def deal_tongrong(fname,fromwho,maildate):
    #把记录放到表szcx.auto_claim_tongrong
    global db_info

    ls_dealresult=''

    li_imported = 0
    li_failure = 0
    li_updateed = 0
    try:
        data = xlrd.open_workbook(fname)
    except Exception as e:
        #print(str(e))
        ls_dealresult = ls_dealresult +fromwho+"---"+maildate.strftime('%a, %d %b %Y %H:%M:%S +0800')+"---"+fname+"---"+"打开附件失败"
        return -1,ls_dealresult
    # 取第一个Sheet
    table = data.sheet_by_index(0)
    nrows = table.nrows

    if nrows > 0:
        global db_error
        if db_error:
            return -1,ls_dealresult + '当前无法连接数据库'
        try:
            con = db.connect(db_info)
        except db.DatabaseError as exc:
            error, = exc.args
            ls_dealresult = ls_dealresult + "Oracle-Error-Code:"+str(error.code)+" Oracle-Error-Message:"+error.message
            db_error = True
            return -1,ls_dealresult
        cur = con.cursor()
        try:
            cur.execute('truncate table szcx.auto_claim_tongrong_cp')
        except db.DatabaseError as exc:
            error, = exc.args
            ls_dealresult = ls_dealresult + "Oracle-Error-Code:"+str(error.code)+" Oracle-Error-Message:"+error.message
            db_error = True
            return -1,ls_dealresult
        con.commit()

        for i in range(nrows):
            if type(table.row_values(i)[8])== type(0.0) or type(table.row_values(i)[8])== type(1)  :
                try:
                    cur.execute('''
                            insert into szcx.auto_claim_tongrong_cp values 
                            (:zd0,:zd1,:zd2,:zd3,:zd4,:zd5,:zd6,:zd7,:zd8,:zd9,:zd10,sysdate,sysdate,:zd11,:zd12)
                            ''',
                            zd0=(table.row_values(i)[0]),
                            zd1=(table.row_values(i)[1]),
                            zd2=datetime.datetime(*xlrd.xldate_as_tuple(table.row_values(i)[2],data.datemode)),
                            zd3=(table.row_values(i)[3]),
                            zd4=(table.row_values(i)[4].replace(' ', '').replace('\n', '').replace('\t', '').replace('\r', '')),
                            zd5=(table.row_values(i)[5]),
                            zd6=(table.row_values(i)[6]),
                            zd7=(table.row_values(i)[7]),
                            zd8=(table.row_values(i)[8]),
                            zd9=(table.row_values(i)[9]),
                            zd10=(table.row_values(i)[10]),
                            zd11=(fromwho),
                            zd12=(maildate))
                except db.DatabaseError as exc:
                    error, = exc.args
                    ls_dealresult = ls_dealresult + '\r\n'+table.row_values(i)[4].replace(' ', '').replace('\n', '').replace('\t', '').replace('\r', '')
                    ls_dealresult = ls_dealresult + '\r\n' + "Oracle-Error-Code:"+str(error.code)+" Oracle-Error-Message:"+error.message
                    li_failure = li_failure + 1
                else:
                    li_imported = li_imported + 1
                    con.commit()
                    #print("以下数据已插入")
                    #print(table.row_values(i),fromwho,maildate)
        try:
            cur.execute('''
                    merge into szcx.auto_claim_tongrong a
                    using (
                    select * from (
                        select c.notificationno,c.insured,c.sectionname,c.serial_no,c.report_date
                        ,c.reporter,before_discuss,c.after_discuss
                        ,c.tr_amount,c.bmmc,c.discuss_reson,c.mail_sender,c.mail_time,c.update_time
                        ,row_number() over(partition by c.notificationno,c.insured order by report_date desc) rn
                        from szcx.auto_claim_tongrong_cp c)
                    where rn = 1                    
                    )b
                    on (a.notificationno=b.notificationno and a.insured=b.insured)
                    when matched
                    then update
                        set 
                         a.sectionname        = b.sectionname   
                        ,a.serial_no          = b.serial_no     
                        ,a.report_date        = b.report_date   
                        ,a.reporter           = b.reporter      
                        ,a.before_discuss     = b.before_discuss
                        ,a.after_discuss      = b.after_discuss 
                        ,a.tr_amount          = b.tr_amount     
                        ,a.bmmc               = b.bmmc          
                        ,a.discuss_reson      = b.discuss_reson 
                        ,a.update_time        = sysdate   
                        ,a.mail_sender        = b.mail_sender   
                        ,a.mail_time          = b.mail_time     
                    where a.mail_sender <> b.mail_sender 
                    and a.mail_time <> b.mail_time
                    when not matched
                    then insert values(
                            b.sectionname,
                            b.serial_no,
                            b.report_date,
                            b.reporter,
                            b.notificationno,
                            b.insured,
                            b.before_discuss,
                            b.after_discuss,
                            b.tr_amount,
                            b.bmmc,
                            b.discuss_reson,
                            sysdate,
                            sysdate,
                            b.mail_sender,
                            b.mail_time)         
        
             ''')
        except db.DatabaseError as exc:
            error, = exc.args
            ls_dealresult = ls_dealresult + '\r\n' + '更新到结果表出错！！！！！请联系数据中心！！！'
            ls_dealresult = ls_dealresult + '\r\n' + "Oracle-Error-Code:"+str(error.code)+" Oracle-Error-Message:"+error.message
        else:
            li_updateed = cur.rowcount
            con.commit()
        con.commit()
        con.close()
        ls_dealresult = ls_dealresult + '\r\n' +"导入数据"+str(li_imported)+"条,失败"+str(li_failure)+"条，更新到最终表"+str(li_updateed)+'条'
        return 1,ls_dealresult

def main():
    global db_info,user,password,pop3_server

    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

    # os.chdir('/home/kettle/python/temp')
    os.chdir('D:/python/fingercaseimport')
    # today=time.strftime('%Y/%m/%d',time.localtime())
    # sub="Fwd: %s - D10 - Daily Details Report & Statement" % (today)
    # print("today:"+today)
    # print("sub:"+sub)
    conf = configparser.ConfigParser()
    conf.read('import.conf')
    db_info = conf['info']['dbinfo']

    # 邮件地址, 口令和POP3服务器地址:
    user = conf['info']['mailuser']
    password = conf['info']['mailpass']
    pop3_server = conf['info']['mailserver']
    # 连接到POP3服务器:
    server = poplib.POP3(pop3_server)
    #server.set_debuglevel(1)  # 调试信息
    # 身份认证:
    try:
        server.user(user)
        server.pass_(password)
    except poplib.error_proto as e:
        print("mail Login failed:", e)
        sys.exit(1)

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
    for i in range(1, index+1):
        deal_mail(server, i,conf)
    server.quit()
if __name__ == '__main__':
    main()
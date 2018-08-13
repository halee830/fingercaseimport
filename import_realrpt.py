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

def xlrd_date(cell_data,datemode):
    if cell_data == "" or cell_data is None:
        return None
    else:
        return datetime.datetime(*xlrd.xldate_as_tuple(cell_data, datemode))

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

    if '全损' in subject:
        fname = get_attachment(msg)
        if fname is None:
            print("全损邮件：", subject,"邮件时间：",maildate,"——获取附件失败")
            return
        li_success, ls_dealmsg = deal_quansun(fname, fromwho, maildate)
        if li_success== 1:
            server.noop()
            server.dele(index)
            ls_dealmsg = "邮件数据处理成功"+'\r\n'+ls_dealmsg
        else:
            ls_dealmsg = "邮件数据处理失败" + '\r\n' + ls_dealmsg
        send_mail(fromwho,receiver,subject,ls_dealmsg)    #收件人中文地址，收件人英文地址，标题，正文

def deal_quansun(fname,fromwho,maildate):
    global db_info
    ls_dealresult = ''

    li_imported = 0
    li_failure = 0
    li_updateed = 0
    try:
        data = xlrd.open_workbook(fname)
    except Exception as e:
        #print(str(e))
        ls_dealresult = ls_dealresult +fromwho+"---"+maildate.strftime('%a, %d %b %Y %H:%M:%S +0800')+"---"+fname+"---"+"打开附件失败"
        return -1,ls_dealresult
    table = data.sheet_by_index(0)
    nrows = table.nrows
    if nrows > 0:
        global db_error
        if db_error:
            return -1, ls_dealresult + '当前无法连接数据库'
        try:
            con = db.connect(db_info)
        except db.DatabaseError as exc:
            error, = exc.args
            ls_dealresult = ls_dealresult + "Oracle-Error-Code:" + str(error.code) + " Oracle-Error-Message:" + error.message
            db_error = True
            return -1, ls_dealresult
        cur = con.cursor()
        try:
            cur.execute('truncate table szcx.auto_claim_quansun_cp')
        except db.DatabaseError as exc:
            error, = exc.args
            ls_dealresult = ls_dealresult + "Oracle-Error-Code:" + str(error.code) + " Oracle-Error-Message:" + error.message
            db_error = True
            return -1, ls_dealresult
        con.commit()

        for i in range(nrows):
            if table.row_values(i)[10].replace(' ', '').replace('\n', '').replace('\t', '').replace('\r', '') == '深圳分公司':
                try:
                    cur.execute('''
                            insert into szcx.auto_claim_quansun_cp values 
                            (:zd0,:zd1,:zd2,:zd3,:zd4,:zd5,:zd6,:zd7,:zd8,:zd9,:zd10,:zd11,:zd12,:zd13,:zd14,:zd15,:zd16,:zd17,:zd18,:zd19,:zd20,:zd21,:zd22,:zd23,:zd24,:zd25,:zd26,:zd27,:zd28,:zd29,:zd30,:zd31,:zd32,:zd33,:zd34,:zd35,:zd36,:zd37,:zd38,:zd39,:zd40,:zd41,:zd42,sysdate,sysdate,:zd45,:zd46)
                            ''',
                                zd0=xlrd_date(table.row_values(i)[0],data.datemode),
                                zd1=(table.row_values(i)[1]),
                                zd2=(table.row_values(i)[2]),
                                zd3=(table.row_values(i)[3].replace(' ', '').replace('\n', '').replace('\t', '').replace('\r', '')),
                                zd4=(table.row_values(i)[4]),
                                zd5=xlrd_date(table.row_values(i)[5],data.datemode),
                                zd6=xlrd_date(table.row_values(i)[6],data.datemode),
                                zd7=(table.row_values(i)[7]),
                                zd8=(table.row_values(i)[8]),
                                zd9=(table.row_values(i)[9]),
                                zd10=(table.row_values(i)[10]),
                                zd11=(table.row_values(i)[11]),
                                zd12=(table.row_values(i)[12]),
                                zd13=(table.row_values(i)[13]),
                                zd14=(table.row_values(i)[14]),
                                zd15=(table.row_values(i)[15].replace(' ', '').replace('\n', '').replace('\t', '').replace('\r', '')),
                                zd16=xlrd_date(table.row_values(i)[16],data.datemode),
                                zd17=(table.row_values(i)[17]),
                                zd18=(table.row_values(i)[18]),
                                zd19=(table.row_values(i)[19]),
                                zd20=(table.row_values(i)[20]),
                                zd21=(table.row_values(i)[21]),
                                zd22=(table.row_values(i)[22]),
                                zd23=(table.row_values(i)[23]),
                                zd24=(table.row_values(i)[24]),
                                zd25=(table.row_values(i)[25]),
                                zd26=(table.row_values(i)[26]),
                                zd27=(table.row_values(i)[27]),
                                zd28=(table.row_values(i)[28]),
                                zd29=(table.row_values(i)[29]),
                                zd30=(table.row_values(i)[30]),
                                zd31=(table.row_values(i)[31]),
                                zd32=(table.row_values(i)[32]),
                                zd33=xlrd_date(table.row_values(i)[33],data.datemode),
                                zd34=xlrd_date(table.row_values(i)[34],data.datemode),
                                zd35=xlrd_date(table.row_values(i)[35],data.datemode),
                                zd36=xlrd_date(table.row_values(i)[36],data.datemode),
                                zd37=(table.row_values(i)[37]),
                                zd38=(table.row_values(i)[38]),
                                zd39=(table.row_values(i)[39]),
                                zd40=(table.row_values(i)[40]),
                                zd41=(table.row_values(i)[41]),
                                zd42=(table.row_values(i)[42]),
                                zd45=(fromwho),
                                zd46=(maildate))
                except db.DatabaseError as exc:
                    error, = exc.args
                    ls_dealresult = ls_dealresult + '\r\n' + table.row_values(i)[4].replace(' ', '').replace('\n',
                                                                                                             '').replace(
                        '\t', '').replace('\r', '')
                    ls_dealresult = ls_dealresult + '\r\n' + "Oracle-Error-Code:" + str(
                        error.code) + " Oracle-Error-Message:" + error.message
                    li_failure = li_failure + 1
                else:
                    li_imported = li_imported + 1
                    con.commit()
                    # print("以下数据已插入")
                    # print(table.row_values(i),fromwho,maildate)
        try:
            cur.execute('''
                    merge into szcx.auto_claim_quansun a
                    using (
                    select * from (
                        select c.*
                        ,row_number() over(partition by c.notificationno,c.veh_plate_number order by ask_price_start desc) rn
                        from szcx.auto_claim_quansun_cp c)
                    where rn = 1                    
                    )b
                    on (a.notificationno=b.notificationno and a.veh_plate_number = b.veh_plate_number )
                    when matched
                    then update
                        set 
                         a.notificationtime = b.notificationtime
                        ,a.serial_no = b.serial_no
                        ,a.branch_name = b.branch_name
                        ,a.ask_price_no = b.ask_price_no
                        ,a.ask_price_start = b.ask_price_start
                        ,a.ask_price_end = b.ask_price_end
                        ,a.ask_price_valid = b.ask_price_valid
                        ,a.ins_branch = b.ins_branch
                        ,a.ins_bmz = b.ins_bmz
                        ,a.ck_branch = b.ck_branch
                        ,a.ck_bmz = b.ck_bmz
                        ,a.ask_branch = b.ask_branch
                        ,a.veh_usage = b.veh_usage
                        ,a.veh_brand_model = b.veh_brand_model
                        ,a.veh_first_registration_date = b.veh_first_registration_date
                        ,a.is_insured = b.is_insured
                        ,a.veh_frame_number = b.veh_frame_number
                        ,a.veh_serial_number = b.veh_serial_number
                        ,a.veh_value_new = b.veh_value_new
                        ,a.veh_acc_value = b.veh_acc_value
                        ,a.veh_ins_value = b.veh_ins_value
                        ,a.all_damage_loss = b.all_damage_loss
                        ,a.repair_cost = b.repair_cost
                        ,a.damage_type = b.damage_type
                        ,a.m6_all_damage_loss = b.m6_all_damage_loss
                        ,a.normal_repair_cost = b.normal_repair_cost
                        ,a.top_auction_floor = b.top_auction_floor
                        ,a.auction_comp = b.auction_comp
                        ,a.is_top = b.is_top
                        ,a.is_deduction = b.is_deduction
                        ,a.auction_deduction = b.auction_deduction
                        ,a.commercial_close_date = b.commercial_close_date
                        ,a.close_deduction_date = b.close_deduction_date
                        ,a.claim_date = b.claim_date
                        ,a.audit_date = b.audit_date
                        ,a.repair_station = b.repair_station
                        ,a.survey_name = b.survey_name
                        ,a.audit_name = b.audit_name
                        ,a.ask_price_status = b.ask_price_status
                        ,a.accident_type = b.accident_type
                        ,a.is_system_start = b.is_system_start
                        ,a.update_time        = sysdate   
                        ,a.mail_sender        = b.mail_sender   
                        ,a.mail_time          = b.mail_time     
                    where a.mail_sender <> b.mail_sender 
                    and a.mail_time <> b.mail_time
                    when not matched
                    then insert values(
                            b.notificationtime
                            ,b.serial_no
                            ,b.branch_name
                            ,b.notificationno
                            ,b.ask_price_no
                            ,b.ask_price_start
                            ,b.ask_price_end
                            ,b.ask_price_valid
                            ,b.ins_branch
                            ,b.ins_bmz
                            ,b.ck_branch
                            ,b.ck_bmz
                            ,b.ask_branch
                            ,b.veh_usage
                            ,b.veh_brand_model
                            ,b.veh_plate_number
                            ,b.veh_first_registration_date
                            ,b.is_insured
                            ,b.veh_frame_number
                            ,b.veh_serial_number
                            ,b.veh_value_new
                            ,b.veh_acc_value
                            ,b.veh_ins_value
                            ,b.all_damage_loss
                            ,b.repair_cost
                            ,b.damage_type
                            ,b.m6_all_damage_loss
                            ,b.normal_repair_cost
                            ,b.top_auction_floor
                            ,b.auction_comp
                            ,b.is_top
                            ,b.is_deduction
                            ,b.auction_deduction
                            ,b.commercial_close_date
                            ,b.close_deduction_date
                            ,b.claim_date
                            ,b.audit_date
                            ,b.repair_station
                            ,b.survey_name
                            ,b.audit_name
                            ,b.ask_price_status
                            ,b.accident_type
                            ,b.is_system_start
                            ,sysdate
                            ,sysdate
                            ,b.mail_sender
                            ,b.mail_time)         

             ''')
        except db.DatabaseError as exc:
            error, = exc.args
            ls_dealresult = ls_dealresult + '\r\n' + '更新到结果表出错！！！！！请联系数据中心！！！'
            ls_dealresult = ls_dealresult + '\r\n' + "Oracle-Error-Code:" + str(
                error.code) + " Oracle-Error-Message:" + error.message
        else:
            li_updateed = cur.rowcount
            con.commit()
        con.commit()
        con.close()
        ls_dealresult = ls_dealresult + '\r\n' + "导入数据" + str(li_imported) + "条,失败" + str(
            li_failure) + "条，更新到最终表" + str(li_updateed) + '条'
        return 1, ls_dealresult
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
                            zd5=(table.row_values(i)[5].replace(' ', '').replace('\n', '').replace('\t', '').replace('\r', '')),
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
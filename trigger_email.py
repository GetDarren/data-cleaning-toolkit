class AutoEmail(object):
    
    def __init__(self):
        pass
 
    def autoemail(self,messagecontent,Sub_head,files):
        
        mail_host = 'relay.sing.micron.com'
        mail_user = 'darrenzhou@micron.com'#'wanglia@micron.com'
        mail_pwd = 'Style111'
        mail_to = ['darrenzhou@micron.com',"wendyzhao@micron.com"]
        msg = MIMEMultipart()
        header = Sub_head
        message = str(messagecontent)
        for f in files:
            with open(f,'rb') as fil:
                part = MIMEApplication(fil.read(), Name=basename(f))
            part['Content-Disposition']='attachment;filename="{}"'.format(basename(f))
            msg.attach(part)
        body = MIMEText(message)
        msg.attach(body)
        msg['To'] = ','.join(mail_to)
        msg['from'] = mail_user
        msg['subject'] = Header(header,'utf-8')
     
        try:
            s = smtplib.SMTP()
            s.set_debuglevel(1)
            s.connect(mail_host)
            s.starttls()
            s.login(mail_user,mail_pwd)
            s.sendmail(mail_user,mail_to,msg.as_string())
            s.close()
        except Exception as e:  # in Python 2 :   except Exception,e
            print(e)
            
            
Email = AutoEmail()
messagecontent = ""
Sub_head = ""
files = []
Email.autoemail(messagecontent,Sub_head,files)
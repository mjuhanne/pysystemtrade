import smtplib

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

from sysdata.config.production_config import get_production_config


def send_mail_file(textfile, subject):
    """
    Sends an email of a particular text file with subject line

    """

    fp = open(textfile, "rb")
    # Create a text/plain message
    msg = MIMEText(fp.read())
    fp.close()

    _send_msg(msg, subject)


def send_mail_msg(body, subject):
    """
    Sends an email of particular text file with subject line

    """

    # Create a text/plain message
    msg = MIMEMultipart()

    msg.attach(MIMEText(body, "plain"))

    _send_msg(msg, subject)


def send_mail_html(body, body_html, subject):
    """
    Sends an email of particular text file with subject line

    """

    # Create a combined text/plain and text/html message. The former is backup in case e-mail reader can't render html
    msg = MIMEMultipart("alternative")

    msg.attach(MIMEText(body, "plain"))
    msg.attach(MIMEText(body_html, "html"))

    _send_msg(msg, subject)



def send_mail_pdfs(preamble, filelist, subject):
    """
    Sends an email of files with preamble and subject line

    """

    # Create a text/plain message
    msg = MIMEMultipart()
    msg.preamble = preamble

    for file in filelist:
        fp = open(file, "rb")
        attach = MIMEApplication(fp.read(), "pdf")
        fp.close()
        attach.add_header("Content-Disposition", "attachment", filename="file.pdf")
        msg.attach(attach)

    _send_msg(msg, subject)


def _send_msg(msg, subject):
    """
    Send a message composed by other things

    """

    email_server, email_address, email_pwd, email_to, email_port, email_prefix = get_email_details()

    me = email_address
    you = email_to
    msg["From"] = me
    msg["To"] = you
    if email_prefix != "":
        msg["Subject"] = email_prefix + ": " + subject
    else:
        msg["Subject"] = subject

    # Send the message via our own SMTP server, but don't include the
    # envelope header.
    s = smtplib.SMTP(email_server, email_port)
    # add tls for those using yahoo or gmail.
    try:
        s.starttls()
    except:
        pass
    s.login(email_address, email_pwd)
    s.sendmail(me, [you], msg.as_string())
    s.quit()


def get_email_details():
    # FIXME DON'T LIKE RETURNING ALL THESE VALUES - return CONFIG or subset?
    try:
        production_config = get_production_config()
        email_address = production_config.email_address
        email_pwd = production_config.email_pwd
        email_server = production_config.email_server
        email_to = production_config.email_to
        email_port = production_config.email_port
    except:
        raise Exception(
            "Need to have all of these for email to work in private config: email_address, email_pwd, email_server, email_to",
            "email_port",
        )

    email_prefix = ""
    try:
        email_prefix = production_config.email_prefix
    except:
        pass

    return email_server, email_address, email_pwd, email_to, email_port, email_prefix

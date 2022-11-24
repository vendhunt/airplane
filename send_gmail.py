from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from email.mime.text import MIMEText
from base64 import urlsafe_b64encode


"""
this function configures the gmail api 
the function returns an authorized Gmail API service instance
token_path = path to token
key pat = path to key 
"""


def setup_credentials(var_name):
  API_scopes = ['https://mail.google.com/']
  creds = Credentials(
              token=os.environ.get(var_name + '_token'),
              refresh_token=os.environ.get(var_name + '_refresh_token'),
              token_uri="https://www.googleapis.com/oauth2/v3/token", 
              client_id=os.environ.get(var_name + '_client_id'),
              client_secret=os.environ.get(var_name + '_client_secret'),
          )
  gmail_service = build("gmail", "v1", credentials=creds)
  return gmail_service


""" 
sender_mail = email address of sender
send_to = email address of receiver
msg = email message
subject = email subject
message_id = e.g '<CAK0c-uyrkZQZ-xKQ2c4AWwRp258VgHcogW7XhdJgyPzg6Jz3Kw@mail.gmail.com>'
This fuctions builds up the message components
"""


def build_msg(destination, subject, body, html_tag, thread_id=None, message_id=None, sender_mail="me"):
    if html_tag == 1:
        message = MIMEText(body, 'html')
    else:
        message = MIMEText(body)

    message['to'] = destination
    message['from'] = sender_mail
    message['subject'] = subject

    if message_id:
        message['In-Reply-To'] = message_id
        message['References'] = message_id
        raw = urlsafe_b64encode(message.as_bytes()).decode()
        message_tosend = {'raw': raw, 'threadId': thread_id}
    else:
        message_tosend = {'raw': urlsafe_b64encode(
            message.as_bytes()).decode()}

    return message_tosend


"""
Function to send an email.
this function returns the threadID
"""


def sendMail(to, msg, subject, service, html=False, followup=False, threadlabel=None, MessageIDLabel=None,sender="me"):
    try:
        if html:
            html_tag = 1
        else:
            html_tag = 0
        try:

            if followup:
                # get the existing threadID
                thread_id = threadlabel
                message_id = MessageIDLabel
                # get the message-ID
                getdata = service.users().messages().get(userId="me", id=thread_id).execute()
                messageID_main = getdata['payload']['headers'][8]['value']
                # Reply to thread
                message = service.users().messages().send(userId="me", body=build_msg(
                    to, subject, msg, html_tag, thread_id, messageID_main,sender)).execute()
                return thread_id, message_id, messageID_main
            elif not followup:
                message = service.users().messages().send(
                    userId="me", body=build_msg(to, subject, msg, html_tag,sender_mail=sender)).execute()
                thread_id = message['threadId']
                message_id = message['id']

                getdata = service.users().messages().get(userId="me", id=thread_id).execute()
                messageID_main = getdata['payload']['headers'][8]['value']
                return thread_id, message_id, messageID_main
        except Exception as followUpError:
            print("A followUpError has occured: {}".format(followUpError))
    except Exception as SendMailerror:
        print("A Send mail Error has occured:{}".format(SendMailerror))


def sendFollowUp(to, thread_id, Message_id,sender,service,body, subject):
    response_status = checkMailBox(thread_id, to,service)
    # checks whether to send a followup or not
    if response_status >= 0:
        if response_status == 0:
            try:
                sendMail(to, body, subject,service=service, followup=True,
                         threadlabel=thread_id, MessageIDLabel=Message_id,sender=sender)
                return "Sent"
            except Exception as e:
                print("Error Sending Message:", e)
        elif response_status > 0:
            print("Responses Already recieved from  :", to)
            return "N/A"
    else:
        print("No data available")


"""this function returns 0: if no email was received from the recipent otherwise +1"""


def checkMailBox(thread_id, user_email, service):
    # outbox_query = "to:{} ".format(user_email)
    inbox_query = "from:{} ".format(user_email)
    try:
        # get the treadId earliest datetime
        # the Message_thread_response returns a payload dictionary
        message_thread_response = service.users().threads().get(
            userId="me", id=thread_id).execute()
        internalDate = message_thread_response['messages'][0]['internalDate']
        # search messages from usermail
        inbox_response = service.users().messages().list(
            userId="me", q=inbox_query).execute()
        id_list = []
        status = 0
        number_responses = inbox_response['resultSizeEstimate']
        if number_responses > 0:
            msg = inbox_response['messages']
            for ids in msg:
                id_list.append(ids['id'])

            # get the last time a response was recieved from usermail
            for msg_id in id_list:

                response = service.users().messages().get(
                    userId="me", id=msg_id, format='raw').execute()
                resposeDate = response['internalDate']
                if int(resposeDate) > int(internalDate):
                    status += 1
            return status
        else:
            print("No messages found  for user: ", user_email)
            return status

    except Exception as e:
        print("Error has occured: {}".format(e))
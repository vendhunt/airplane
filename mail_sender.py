def main(params):
  import os 
  import send_gmail as gm
  import time 


  service = gm.setup_credentials('topvendingsolutions')

  email = 'alexbreen7@gmail.com'
  msg = 'Howdy! This is a test email'
  subject = 'Howdy Alex!'



  # thread_id, msg_id, msg2 = gm.sendMail(email, msg, subject,
  #                                     service, html=True, sender="Alex Breen<alex@bnbuyer.com>")
  # print(thread_id), 
  # print(msg_id)
  # print(msg2)

  # time.sleep(120)

  thread_id = '184a6ad65c5ba3d3'
  msg_id = '184a6ad65c5ba3d3'
  msg2 = '<CAF7b6nnEL35g4pJ0R9p_j2Px5u5WF8Rs_L1iX-90BeP97DoWjg@mail.gmail.com>'

  followup_msg = 'Just following up on this '

  gm.sendFollowUp(to=email, 
    service = service ,  
    thread_id= thread_id, 
    Message_id=msg2, 
    sender = 'Alex Breen<alex@bnbuyer.com>',
    body=followup_msg,
    subject = 'Howdy Alex!')

main('alex')
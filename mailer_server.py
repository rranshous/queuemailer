

## listens on the queue for
# new mail messages

import zlib
from configsmash import ConfigSmasher
from utils.kawaiiqueue import KawaiiQueueClient
from utils.maillib import Mail, emailError
from time import sleep
from base64 import b64decode

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

EXCEPTION_TEMPLATE = """Exception: %s
Message label: %s
Message body: %s"""

class MailerException(Exception):
    def __init__(self,error_msg,qmsg):
        self.error_msg = error_msg
        self.qmsg = qmsg

    def __str__(self):
        return self.error_msg

class QueueMailer(object):
    """
    sits on queue, mailing given msgs
    """

    def __init__(self,
                queue, # flexqueue compatible queue
                server,port,username,password,sender): # smtp info

        self.queue = queue
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.sender = sender

        # sleep time between queue exhaustions
        self.sleep_time = 1

    def start(self):

        log.debug('mailer starting')
        
        # continue to check the queue forever
        while True:

            try:
                self._loop()

            # ohh nooos
            except MailerException, ex:
                log.error(EXCEPTION_TEMPLATE % (ex,
                                                ex.qmsg.label,
                                                ex.qmsg.body))

            except emailError, ex:
                log.error(EXCEPTION_TEMPLATE % (ex,'',''))

            log.debug('sleeping for %s(s)' % self.sleep_time)

            # out of msgs ? time to sleep
            sleep(self.sleep_time)

            # sleep a lil bit longer each time
            self.sleep_time += 1

    def _loop(self):

        # try and pull some messages from the queue
        for qmsg in self.queue:

            log.debug('received message')

            # they can either pass in a template
            template_text = qmsg.body.get('template_text')

            log.debug('template text: %s' % template_text)

            # or they could have passed in the name
            # of a template we should already have
            template_path = qmsg.body.get('template_path')

            log.debug('template path: %s' % template_path)

            # the template will be in our template dir
            if template_path:
                template_path = os.path.join(self.template_root,
                                             template_path)

            log.debug('template path with root: %s'
                      % template_path)

            # or they could have simply given us the body
            if not template_text:
                log.debug('updating template text from body')
                template_text = qmsg.body.get('body')
                log.debug('template text: %s'
                          % template_text)

            # a queue message must contain some sort of
            # message body
            if not template_text and not template_path:
                raise MailerException('No message body',qmsg)
            
            log.debug('creating mail')

            # figure out what kwargs we are sending to mail
            mail_args = {

                # settings from config
                'server':self.server,
                'username':self.username,
                'password':self.password,
                'port':self.port,
                'sender':self.sender,

                # settings from qmsg
                'to':qmsg.body.get('to'),
                'subject':qmsg.body.get('subject'),
                'replacement_dict':qmsg.body.get('template_args',{})
            }

            # if we have the template text than give them that
            if template_text:
                mail_args['template_text'] = template_text

            # if template path is set, add it
            if template_path:
                mail_args['template_path'] = template_path

            # create our mail item from our kwargs
            mail = Mail(**mail_args)

            log.debug('server: %s' % mail.server)
            log.debug('username: %s' % mail.username)
            log.debug('port: %s' % mail.smtp_port)
            log.debug('sender: %s' % mail.sender)
            log.debug('to: %s' % mail.to)
            log.debug('subject: %s' % mail.subject)
            log.debug('template text: %s' % mail.template_text)
            log.debug('template path: %s' % mail.template_path)
            log.debug('replacement_dict: %s' 
                      % mail.replacement_dict)

            # the attachments should come w/ meta data
            # and gzip'd
            for attachment_data in qmsg.body.get('attachments',[]):

                log.debug('adding attachment')

                # hopefully we got gzipped data, but maybe not
                if attachment_data.get('gzip_data'):
                    log.debug('data gziped')

                    # get the data
                    data = attachment_data.get('gzip_data')

                    # decode it
                    data = b64decode(data)

                    # decompress it
                    data = zlib.decompress(data)

                else:
                    # pull the data
                    data = attachment_data.get('data')

                    # decode our data
                    data = b64decode(data)

                # they should have passed a name as well
                name = attachment_data.get('name')

                log.debug('attachment info: %s %s' % (name,len(data)))

                # add our attachment
                mail.add_attachment(name=name,
                                    data=data)
                
            log.debug('sending it off!')

            # send it off!
            mail.send()

            log.debug('sent!')



if __name__ == '__main__':

    log.debug('starting')
    log.debug('reading config')

    # read in our config
    config = ConfigSmasher(['configs']).smash()

    log.debug('config: %s' % config)
    log.debug('creating queue')

    # create our queue
    queue_config = config.get('queue')
    queue = KawaiiQueueClient(queue_config.get('name'),
                              queue_config.get('host'),
                              queue_config.get('port'))

    log.debug('creating mailer')

    # now create our mailer
    smtp_config = config.get('smtp')
    mailer = QueueMailer(queue,
                         smtp_config.get('server'),
                         smtp_config.get('port'),
                         smtp_config.get('username'),
                         smtp_config.get('password'),
                         smtp_config.get('sender'))

    log.debug('starting mailer')
    mailer.start()

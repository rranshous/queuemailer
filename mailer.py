

## listens on the queue for
# new mail messages

from configsmash import ConfigSmasher
from utils.kawaiiqueue import KawaiiQueueClient
from utils.maillib import Mail, emailError

class Mailer(object):
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

    def run(self):
        
        # continue to check the queue forever
        while True:

            # try and pull some messages from the queue
            for qmsg in self.queue.read_message():

                # we want to report all errors
                try:

                    # they can either pass in a template
                    template_text = qmsg.body.get('template_text')

                    # or they could have passed in the name
                    # of a template we should already have
                    template_path = qmsg.body.get('template_path')

                    # the template will be in our template dir
                    if template_path:
                        template_path = os.path.join(self.template_root,
                                                     template_path)

                    # or they could have simply given us the body
                    if not template_text:
                        template_text = qmsg.body.get('body')

                    # a queue message must contain some sort of
                    # message body
                    if not template_text and not template_path:
                        raise MailerException('No message body')
                    
                    # create our mail item
                    mail = Mail(

                            # settings from config
                            server=self.server,
                            username=self.username,
                            password=self.password,
                            smtp_port=self.port,
                            sender=self.sender,

                            # settings from qmsg
                            to=qmsg.body.to,
                            subject=qmsg.body.subject,
                            template_text=template_text,
                            template_path=template_path,
                            replacement_dict=qmsg.body.template_args

                    )

                # ohh nooos
                except (MailerException, emailError), ex:
                    log.error(EXCEPTION_TEMPLATE % (ex,
                                                    qmsg.label,
                                                    qmsg.body)

            # out of msgs ? time to sleep
            sleep(self.sleep_time)
            # sleep a lil bit longer each time
            self.sleep_time += 1



if __name__ == '__main__':

    # read in our config
    config = ConfigSmasher('configs').smash()

    # create our queue
    queue_config
    queue = KawaiiQueueServer(queue_config.get('name'),
                              queue_config.get('host'),
                              queue_config.get('port'))

    # now create our mailer
    smtp_config = config.get('smtp')
    mailer = Mailer(queue,
                    smtp_config.get('server'),
                    smtp_config.get('port'),
                    smtp_config.get('username'),
                    smtp_config.get('password'),
                    smtp_config.get('sender'))

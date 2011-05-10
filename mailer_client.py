#!/usr/bin/python

"""
sends msg to send an email
"""

import zlib
import argparse
from configsmash import ConfigSmasher
from cStringIO import StringIO
from findfiles import find_files_iter as find_files
import zipfile
from utils.kawaiiqueue import KawaiiQueueClient
import os.path
from base64 import b64encode

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


class MailerClient(object):

    def __init__(self,queue):

        # flexqueue compatible
        self.queue = queue
    
    def queue_mail(self, to, subject, body, attachment_paths=None):
        """
        adds a msg to the email queue, attachment paths which are
        dirs will be zipped when attached
        """


        # compile our data to send to the queue
        msg_data = {
            'to':to,
            'subject':subject,
            'body':body
        }

        log.debug('base message: %s' % msg_data)
        log.debug('adding attachments')

        # create entries for our attachments
        attachments = []
        for path in attachment_paths:

            log.debug('attachment path: %s' % path)

            # get it's name
            if path.endswith('/'):
                path = path[:-1] # it's a dir
            name = os.path.basename(path)

            log.debug('attachment name: %s' % name)

            # get the full path
            path = os.path.abspath(os.path.expanduser(path))

            log.debug('attachment full path: %s' % path)

            # if it's a dir than we are going to zip the contents
            if os.path.isdir(path):
                
                log.debug('attachment is dir')

                # we want to zip the entire directory in memory
                # setup an in memory file object
                zip_fh = StringIO()

                log.debug('creating our zip')

                # create our new zip
                _zip = zipfile.ZipFile(zip_fh,'a',
                                       zipfile.ZIP_DEFLATED,
                                       False)

                log.debug('going through files')

                # go through all the files in the dir, recursively
                for file_path in find_files(path):

                    log.debug('file path: %s' % file_path)
                    
                    # use it's rel file name from the attachment path
                    # as it's name in the zip
                    rel_file_path = file_path[len(path):]

                    # make sure it starts w/ a slash
                    if not rel_file_path.startswith('/'):
                        rel_file_path = '/%s' % rel_file_path

                    log.debug('rel file path: %s' % rel_file_path)
                    log.debug('adding file data to zip')

                    # add it to our zip
                    with file(file_path,'r') as fh:
                        _zip.writestr(rel_file_path,fh.read())

                # the attachment name needs to end in .zip
                name = '%s.zip' % name

                log.debug('updating zipped files created flag')

                # Mark the files as having been created on Windows so that
                # Unix permissions are not inferred as 0000
                for zfile in _zip.filelist:
                    zfile.create_system = 0 

                log.debug('reading zip from memory')

                # get our zip's data (closing it's buffer)
                zip_fh.seek(0)
                data = zip_fh.read()
                _zip.close()
                zip_fh.close()

                # encode our data
                data = b64encode(data)

                log.debug('adding to attachments list')
                
                # add it to our attachment list
                attachments.append({
                    'name':name,
                    'data':data
                })

            # it's not a path, just a single file
            else:

                log.debug('reading attachment file')

                # read in and gzip the data
                with file(path,'r') as fh:

                    log.debug('reading data')

                    # for now skip the compression
                    data = fh.read()
                    #data = zlib.compress(fh.read())

                    # encode our data
                    data = b64encode(data)

                    log.debug('adding to attachment list')

                    # add the data to our list
                    attachments.append({
                       'name':name,
                       #'gzip_data':data
                       'data':data
                    })

        # if we have any attachments, add them to the message
        if attachments:
            log.debug('adding attachments to message')
            msg_data['attachments'] = attachments

        log.debug('sending message to queue')

        # add it to the queue
        self.queue.send_message('email',msg_data)

if __name__ == '__main__':
    log.debug('starting')

    # command line parsing!
    parser = argparse.ArgumentParser(description='Add email to queue')

    # to is going just be comma seperated email addresses
    parser.add_argument('to',help='comma seperated email addresses')

    # subject, simple string
    parser.add_argument('subject',help='subject of email')

    # the body, simple, straitforward
    parser.add_argument('body',help='body of email')

    # the rest of the args are file / dir paths
    parser.add_argument('attachment_paths',nargs='*',
                        help='attach files / dirs')

    # parse those cmd line options!
    log.debug('parsing args')
    args = parser.parse_args()

    log.debug('args: %s' % args)
    log.debug('reading config')

    # read in from the configs
    config = ConfigSmasher(['configs']).smash()

    log.debug('config: %s' % config)
    log.debug('creating queue')

    # create our queue
    queue_config = config.get('queue')
    queue = KawaiiQueueClient(queue_config.get('name'),
                              queue_config.get('host'),
                              queue_config.get('port'))

    log.debug('creating mailer')

    # create our mailer, attached to the new queue
    mailer = MailerClient(queue)

    log.debug('queueing mail')

    # have the mailer send the msg to the queue
    mailer.queue_mail(args.to,
                      args.subject,
                      args.body,
                      args.attachment_paths)

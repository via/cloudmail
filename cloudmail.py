import base64, urllib2, sys, re, time

from twisted.internet import reactor
from twisted.mail import imap4
from twisted.mail.smtp import rfc822date
from zope.interface import implements
from twisted.internet import reactor, defer, protocol
from twisted.cred import checkers, credentials, error as credError
from urlparse import urlparse
from urllib2 import HTTPError
from email.Parser import Parser
from email.Generator import Generator
from cStringIO import StringIO
import random
import redis
import fnmatch

_statusRequestDict = {
    'MESSAGES': 'getMessageCount',
    'RECENT': 'getRecentCount',
    'UIDNEXT': 'getUIDNext',
    'UIDVALIDITY': 'getUIDValidity',
    'UNSEEN': 'getUnseenCount'
}

REMOVE_FLAGS = -1
SET_FLAGS = 0 
ADD_FLAGS = 1

MAILBOXDELIMITER = "."
#The initial Mail Boxes, None's will be replaced with MailBox objects

#The user account wrapper
class CloudFSUserAccount(object):
  implements(imap4.IAccount)

  def __init__(self, username):
    #The cache we use to pass data from one class to another
    self.user = username
    #DB connection
    self.pool = redis.ConnectionPool(host='redis1.redis')
    self.conn = redis.Redis(connection_pool=self.pool)
    self.defflags=["\HasNoChildren"]
    
  #Get all the mailboxes and setup the SQL DB
  def listMailboxes(self, ref, wildcard):
    boxes = self.conn.smembers("%s:mailboxes" % self.user)
    print self.user
    mail_boxes = []
    for i in boxes:
        newbox = CloudFSImapMailbox(self.user, i, self.pool)
        if fnmatch.fnmatch(i, ref + wildcard):
          mail_boxes.append((i, newbox))
    
    return defer.succeed(mail_boxes)

  #Select a mailbox
  def select(self, path, rw=True):
    print "Select: %s" % path
    if self.conn.sismember( ("%s:mailboxes") % self.user, path):
      return CloudFSImapMailbox(self.user, path, self.pool)
    else:
      return None

  def close(self):
    return True

  def create(self, path):
    pipe = self.conn.pipeline(transaction=True)
    try:
      pipe.watch("%s:mailboxes" % self.user)
      pipe.multi()
      pipe.sadd("%s:mailboxes" % self.user, path)
      pipe.delete("%s:mailboxes:%s:flags" % (self.user, path))
#      pipe.sadd("%s:mailboxes:%s:flags" % (self.user, path), *self.defflags)
      pipe.set("%s:mailboxes:%s:uidvalidity" % (self.user, path),
          random.randint(0, 65535))
      pipe.set("%s:mailboxes:%s:count" % (self.user, path), 0)
      pipe.set("%s:mailboxes:%s:unseen" % (self.user, path), 0)
      pipe.set("%s:mailboxes:%s:recent" % (self.user, path), 0)
      pipe.set("%s:mailboxes:%s:uidnext" % (self.user, path), 1)
      pipe.execute()
    except redis.WatchError:
      return False

    return True

  def delete(self, path):
    pipe = self.conn.pipeline(transaction=True)
    try:
      pipe.watch("%s:mailboxes" % self.user)
      pipe.multi()
      pipe.srem("%s:mailboxes" % self.user, path)
      pipe.delete("%s:mailboxes:%s:flags" % (self.user, path))
      pipe.delete("%s:mailboxes:%s:uidvalidity" % (self.user, path))
      pipe.delete("%s:mailboxes:%s:count" % (self.user, path))
      pipe.delete("%s:mailboxes:%s:recent" % (self.user, path))
      pipe.delete("%s:mailboxes:%s:unseen" % (self.user, path))
      pipe.delete("%s:mailboxes:%s:uidnext" % (self.user, path))
      pipe.execute()
    except redis.WatchError:
      return False
    return True

  def rename(self, oldname, newname):
    return False

  def isSubscribed(self, path):
    return True

  def subscribe(self, path):
    return True

  def unsubscribe(self, path):
    return True

class CloudFSImapMailbox(object):
  implements(imap4.IMailbox)

  def __init__(self, user, path, pool):
    print "Fetching: %s" % path
    self.folder = path
    self.user = user;
    self.listeners = []
    self.pool = pool
    self.conn = redis.Redis(connection_pool=pool)
    #self.conn.sadd("%s:mailboxes:%s:flags" % (self.user, self.folder), "\Seen")
    #self.conn.srem("%s:mailboxes:%s:flags" % (self.user, self.folder),
    #    "\Recent")
    #self.conn.set("%s:mailboxes:%s:recent" % (self.user, self.folder), 0)


  def getHierarchicalDelimiter(self):
    return MAILBOXDELIMITER

  def getFlags(self):
    flags = self.conn.smembers("%s:mailboxes:%s:flags" % (self.user,
      self.folder))
    return ["\Answered", "\Flagged", "\Deleted", "\Seen", "\Draft"]
#    return flags

  def getMessageCount(self):
    messages = self.conn.get("%s:mailboxes:%s:count" % (self.user, self.folder))
    return int(messages)

  def getRecentCount(self):
    messages = self.conn.get("%s:mailboxes:%s:recent" % (self.user, self.folder))
    return int(messages)

  def getUnseenCount(self):
    messages = self.conn.get("%s:mailboxes:%s:unseen" % (self.user, self.folder))
    return int(messages)

  def isWriteable(self):
    return True

  def getUIDValidity(self):
    validity = self.conn.get("%s:mailboxes:%s:uidvalidity" % (self.user, self.folder))
    return int(validity)

  def getUID(self, messageNum):
    return messageNum

  def getUIDNext(self):
    uidnext = self.conn.get("%s:mailboxes:%s:uidnext" % (self.user, self.folder))
    return int(uidnext)

  def fetch(self, messages, uid):
    result = []
    # so, messages is a list of seqnums or uids.
    # if uid, then uis, else seqnums
   
    if uid:
        if not messages.last or messages.last > self.getUIDNext():
            messages.last = self.getUIDNext() -1
    else:
        if not messages.last or messages.last > self.getMessageCount():
            messages.last = self.getMessageCount()

    print "Message.last: ", messages.last

    for id in messages:
        print "ID: ", id
        seq = None
        msg_uid = None
        if uid:
            msg_uid = id
            seq = self.getSeqForUid(id)
        else:
            key = "%s:mailboxes:%s:uidlist" % (self.user, self.folder)
            msg_uid = int(self.conn.zrange(key, id-1, id-1)[0])
            seq = id
        print "message uid: %d    seq: %d   isuid: %d" %( msg_uid, seq, uid)
        result.append((seq, CloudFSImapMessage(self.user, self.folder, msg_uid, self.pool)))

    return result
      
  def getSeqForUid(self, uid):
    key = "%s:mailboxes:%s:uidlist" % (self.user, self.folder)
    seq = self.conn.zrank(key, uid)
    if seq:
      return seq + 1
    raise Exception("%d is not a valid UID." % uid)

  def addListener(self, listener):
    self.listeners.append(listener)
    return True

  def removeListener(self, listener):
    self.listeners.remove(listener)
    return True

  def requestStatus(self, names):
    r = {}
    for n in names:
        r[n] = getattr(self, _statusRequestDict[n.upper()])()
    return r

  def addMessage(self, msg, flags=None, date=None):
    #print "Add Message: %s :: %s" % (msg, flags)
    # passes a file handler here, need to cache fetchBodyFile so I can find the message id.
    #   list of uids
    #     uids[sequence_number] = msg_uid
    #   add new uid
    #     rpush[msg_uid]
    parse = Parser()
    email_obj = parse.parse(msg)
    fp = StringIO()
    g = Generator(fp, mangle_from_=False, maxheaderlen=60)
    g.flatten(email_obj)
    body = fp.getvalue()

    msg_uid = self.conn.incr("%s:mailboxes:%s:uidnext" % (self.user,
      self.folder)) - 1;
    
    seq_number = self.conn.zadd("%s:mailboxes:%s:uidlist"% (self.user, self.folder), msg_uid, msg_uid)

    if not flags:
        flags = ['\Recent']

    self.conn.sadd("%s:mailboxes:%s:mail:%s:flags" % (self.user, self.folder,
      msg_uid), *flags)
    self.conn.set("%s:mailboxes:%s:mail:%s:date" % (self.user, self.folder,
      msg_uid), rfc822date())
    self.conn.incr("%s:mailboxes:%s:count" % (self.user, self.folder))
    self.conn.set("%s:mailboxes:%s:mail:%s:size" % (self.user, self.folder,
      msg_uid), len(body))
    self.conn.set("%s:mailboxes:%s:mail:%s:body" % (self.user, self.folder,
      msg_uid), body)
    self.conn.sadd("%s:mailboxes:%s:mail:%s:headers" % (self.user, self.folder,
      msg_uid), *(email_obj.keys()))
    print email_obj.keys()
    for header in email_obj.keys():
      self.conn.set("%s:mailboxes:%s:mail:%s:header:%s" % (self.user,
        self.folder, msg_uid, header.lower()), email_obj[header])
      print header, msg_uid, self.folder, self.user
    self.conn.incr("%s:mailboxes:%s:recent" % (self.user, self.folder))
    return defer.succeed(seq_number)

  def store(self, messages, flags, mode, uid):
    print "STORE: %s :: %s :: %s :: %s" % (messages, flags, mode, uid)

    if uid:
      if not messages.last or messages.last > self.getUIDNext():
         messages.last = self.getUIDNext() -1
    else:
      if not messages.last or messages.last > self.getMessageCount():
        messages.last = self.getMessageCount()

    print "Message.last: ", messages.last

    # seq => flasgs.str
    result = {}

    for id in messages:
      print "ID: ", id
      seq = None
      msg_uid = None
      if uid:
        msg_uid = id
        seq = self.getSeqForUid(id)
      else:
        key = "%s:mailboxes:%s:uidlist" % (self.user, self.folder)
        msg_uid = int(self.conn.zrange(key, id-1, id-1)[0])
        seq = id
      print "message uid: %d    seq: %d   isuid: %d" %( msg_uid, seq, uid)
      key = "%s:mailboxes:%s:mail:%s:flags" % (self.user, self.folder, msg_uid)

      print "Flags %s" % flags
      if mode == REMOVE_FLAGS:
        self.conn.srem(key, *flags)
      elif mode == SET_FLAGS:
        pipe = self.conn.pipeline(transaction=True)
        pipe.multi()
        pipe.delete(key)
        pipe.sadd(key, *flags)
        pipe.execute()
      elif mode == ADD_FLAGS:
        self.conn.sadd(key, *flags)

      stored_flags = self.conn.smembers(key)

      #account for deleted flag modifications
      del_key = "%s:mailboxes:%s:deleted" % (self.user, self.folder)
      print "Stored flags: " , stored_flags
      if "\\Deleted" in stored_flags:
        print "DELETED"
        self.conn.sadd(del_key, msg_uid)
      else:
        print " NOT DELETED"
        self.conn.srem(del_key, msg_uid)

      result[seq] = stored_flags

    return result

  def expunge(self):
    key = "%s:mailboxes:%s:deleted" % (self.user, self.folder)
    uid = self.conn.spop(key)
    deleted_seqs = []
    while uid:
      deleted_seqs.append(self.getSeqForUid(uid))
      self.expunge_helper(uid)
      uid = self.conn.spop(key)

    return deleted_seqs

  def expunge_helper(self, uid):
    uidlist_key = "%s:mailboxes:%s:uidlist" % (self.user, self.folder)
    self.conn.zrem(uidlist_key, uid)
    glob_key = "%s:mailboxes:%s:mail:%s:*" % (self.user,self.folder, uid)
    keys = self.conn.keys(glob_key)
    print "DELETING: ", keys
    self.conn.delete(*keys)
    # decrement count
    count_key = "%s:mailboxes:%s:count" % (self.user, self.folder)
    self.conn.decr(count_key)
    flags_key = "%s:mailboxes:%s:mail:%s:flags" % (self.user, self.folder, uid)
    stored_flags = self.conn.smembers(key)
    if "\Recent" in stored_flags:
      recent_key = "%s:mailboxes:%s:recent" % (self.user, self.folder)
      self.conn.decr(recent_key)

  def destroy(self):
    raise imap4.MailboxException("Not implemented")
    
class CloudFSImapMessage(object):
  implements(imap4.IMessage)
  
  def __init__(self, user, mailbox, uid, pool):
    #print "MESSAGE: %s" % info
    self.user = user
    self.folder = mailbox
    self.uid = uid
    self.conn = redis.Redis(connection_pool=pool)
    
  def getUID(self):
    return self.uid
    
  def getFlags(self):
    return self.conn.smembers("%s:mailboxes:%s:mail:%s:flags" % (self.user,
      self.folder, self.uid))

    
  def getInternalDate(self):
    #print "getInternalDate :: %s" % self.info
    #return rfc822date(time.localtime(self.info['created_at_in_seconds']))
    return self.conn.get("%s:mailboxes:%s:mail:%s:date" % (self.user,
      self.folder, self.uid))
    
  def getHeaders(self, negate, *names):
    headerdict = {}
    lowernames = map(lambda s: s.lower(), names)
    headers = self.conn.smembers("%s:mailboxes:%s:mail:%s:headers" % (self.user,
      self.folder, self.uid))
    for i in map(lambda s: s.lower(), headers):
      thisheader = self.conn.get("%s:mailboxes:%s:mail:%s:header:%s" %
          (self.user, self.folder, self.uid, i))
      if i in lowernames and not negate:
          headerdict[i] = thisheader
      if i not in lowernames and negate:
          headerdict[i] = thisheader
    print "getHeaders(): ", lowernames, headerdict
    return headerdict
    
  def getBodyFile(self):
    body = self.conn.get("%s:mailboxes:%s:mail:%s:body" % (self.user,
      self.folder, self.uid))
    headerstart = body.find('\r\n\r\n')
    txt = body[headerstart+2:].encode("utf-8")
    return StringIO(txt)
    
  def getSize(self):
    return self.conn.get("%s:mailboxes:%s:mail:%s:size" % (self.user,
      self.folder, self.uid))
    
  def isMultipart(self):
    return False
    
  def getSubPart(self, part):
    print "SUBPART:: %s" % part
    raise imap4.MailboxException("getSubPart not implemented")




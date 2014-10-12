import bisect
import fnmatch
import random
import redis
from httpmail import HTTPMail

from cStringIO import StringIO
from email.Generator import Generator
from email.Parser import Parser
from twisted.cred import checkers, credentials, error as credError
from twisted.internet import reactor, defer, protocol
from twisted.mail import imap4
from twisted.mail.smtp import rfc822date
from txredis.protocol import Redis as txRedis
from txredis.protocol import RedisClientFactory as txRedisClientFactory
from txredis.protocol import RedisSubscriber as txRedisSubscriber

from zope.interface import implements

from threading import Lock

_statusRequestDict = {
    'MESSAGES': 'getMessageCount',
    'RECENT': 'getRecentCount',
    'UIDNEXT': 'getUIDNext',
    'UIDVALIDITY': 'getUIDValidity',
    'UNSEEN': 'getUnseenCount',
    'PERMANENTFLAGS': 'getPermanentFlags'
}

REMOVE_FLAGS = -1
SET_FLAGS = 0 
ADD_FLAGS = 1

MAILBOXDELIMITER = "."

class MailboxSubscriber(txRedisSubscriber):

    def messageReceived(self, channel, message):
        #        print "received %s on %s" % (message, channel)
        (cmd, arg) = message.split(" ")
        if "count" in cmd:
            #print "count!"
            newuid = int(arg)
            uidlist_key = "%s:mailboxes:%s:uidlist" % (self.box.user, self.box.folder)
            uidlist = self.box.conn.zrange(uidlist_key, 0, -1)
            if newuid not in [int(x) for x in uidlist]:
                print "ERROR: added new uid to seq list, but uid doesn't exist"
            self.box.seqlist.append( (newuid, []))
            self.listener.newMessages(self.box.getMessageCount(), None)
        elif "flags" in cmd:
            altereduid = int(arg)
            flags_key = "%s:mailboxes:%s:mail:%s:flags" % (self.box.user,
                    self.box.folder, altereduid)
            try:
                seq = self.box.getSeqForUid(altereduid)
            except  Exception:
                #We were told about flags on a message we're not familiar with
                return
            curflags = self.box.conn.smembers(flags_key)
            self.box.correctDeletedList(curflags, seq)
            mapping = {seq : tuple(curflags)}
            self.listener.flagsChanged(mapping)


    def assignListener(self, listener, box):
        self.listener = listener
        self.box = box

class MailboxSubscriberFactory(txRedisClientFactory):
    protocol = MailboxSubscriber



#The user account wrapper
class HTTPMailAccount(object):
  implements(imap4.IAccount)

  def __init__(self, username):
    #The cache we use to pass data from one class to another
    self.user = username
    #DB connection
    self.conn = HTTPMail("http://localhost:3000")
    self.pool = None
    self.defflags=["\HasNoChildren"]
    
  #Get all the mailboxes and setup the SQL DB
  def listMailboxes(self, ref, wildcard):
    def listcb(boxes):
      mail_boxes = []
      for i in boxes:
          newbox = CloudFSImapMailbox(self.user, i, self.pool)
          if fnmatch.fnmatch(i, ref + wildcard):
            mail_boxes.append((i, newbox))
      return mail_boxes
    d = self.conn.getTags(self.user)
    d.addCallback(listcb)
    return d

  #Select a mailbox
  def select(self, path, rw=True):
      #print "Select: %s" % path
    if path in self.conn.getTags(self.user):
      box = HTTPMailImapMailbox(self.user, path, self.conn)
    else:
      return None

  def printMsg(msg):
      pass
      #print "MSG: ", msg

  def close(self):
    return True

  def create(self, path):
    return self.conn.newTag(self.user, path)

  def delete(self, path):
    return self.conn.deleteTag(self.user, path)

  def rename(self, oldname, newname):
    return False

  def isSubscribed(self, path):
    return False

  def subscribe(self, path):
    return True

  def unsubscribe(self, path):
    return True

class HTTPMailImapMailbox(object):
  implements(imap4.IMailbox)

  def __init__(self, user, path, conn):
      #print "Fetching: %s" % path
    self.folder = path
    self.user = user;
    self.conn = conn

# eventually we could store persistent uidvalidity/uidlist in redis
    self.seqlist = self.conn.getDirectoryMessages(self.user, self.folder)
    self.notifications = None
    self.recent_count = 0
    self.deleted_seqs = []

  def __del__(self):
    #unsubscribe from the flgas we care about.
    pass
     
  def getHierarchicalDelimiter(self):
    return MAILBOXDELIMITER

  def getFlags(self):
    flags = self.conn.smembers("%s:mailboxes:%s:flags" % (self.user,
      self.folder))
    return ["\Answered", "\Flagged", "\Deleted", "\Seen", "\Draft"]
#    return flags

  def getPermanentFlags(self):
      return []

  def getMessageCount(self):
    messages = self.conn.headDirectory(self.user, self.folder)['total']
    return messages

  def getRecentCount(self):
      #messages = self.conn.get("%s:mailboxes:%s:recent" % (self.user, self.folder))
    return self.recent_count

  def getUnseenCount(self):
    messages = self.conn.headDirectory(self.user, self.folder)['unread']
    return messages

  def isWriteable(self):
    return True

  def getUIDValidity(self):
    return 1

  def getUID(self, messageNum):
    return messageNum

  def getUIDNext(self):
    return self.getMessageCount() + 1

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

    #print "Message.last: ", messages.last

    for id in messages:
        #print "ID: ", id
        seq = None
        msg_uid = None
        if uid:
            msg_uid = id
            seq = self.getSeqForUid(id)
        else:
            seq = id
            msg_uid = self.seqlist[seq - 1][0]
        #print "message uid: %d    seq: %d   isuid: %d" %( msg_uid, seq, uid)
        result.append((seq, CloudFSImapMessage(self.user, self.folder, msg_uid,
            self.pool, self.seqlist[seq - 1][1])))

    return result
      
  def getSeqForUid(self, uid):
    #Need to replace this list with something that can do faster lookups
      try:
        uid = int(uid)
        seq = bisect.bisect_left(map(lambda x: x[0], self.seqlist), uid)
        if seq == len(self.seqlist) or self.seqlist[seq][0] != uid:
          print "\n \n Raising ValueError for uid: %d. seq: %d len seqlist:%d. val: %d \n \n" % (uid,
                  seq, len(self.seqlist), self.seqlist[seq][0])
          raise ValueError
        return seq + 1
      except:
        raise Exception("%d is not a valid UID." % uid)
    #print self.seqlist

  @defer.inlineCallbacks
  def addListener(self, listener):
    mailbox_key = "%s:mailboxes:%s:channel" % (self.user, self.folder)
    clientCreator = protocol.ClientCreator(reactor, MailboxSubscriber)
    listener.connection = yield clientCreator.connectTCP('localhost', 6379)
    listener.connection.assignListener(listener, self)
    yield listener.connection.subscribe(mailbox_key)

    self.listeners.append(listener)
    #print "Listener Added"
    defer.returnValue(True)

  def removeListener(self, listener):
    mailbox_key = "%s:mailboxes:%s:channel" % (self.user, self.folder)
    #print "Listener Deleted"
    self.listeners.remove(listener)
    listener.connection.unsubscribe(mailbox_key)
    del listener.connection.box
    del listener.connection
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
    
    self.conn.zadd("%s:mailboxes:%s:uidlist"% (self.user, self.folder), msg_uid, msg_uid)

    self.seqlist.append((msg_uid, ['\Recent']))
    seq_number = len(self.seqlist)

    if flags:
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
    #print email_obj.keys()
    for header in email_obj.keys():
      self.conn.set("%s:mailboxes:%s:mail:%s:header:%s" % (self.user,
        self.folder, msg_uid, header.lower()), email_obj[header])
      #print header, msg_uid, self.folder, self.user
    self.conn.incr("%s:mailboxes:%s:recent" % (self.user, self.folder))
    self.recent_count += 1
    self.conn.publish("%s:mailboxes:%s:channel" % (self.user, self.folder),
            "count %d" % (msg_uid))
    return defer.succeed(seq_number)

  def store(self, messages, flags, mode, uid):
      #print "STORE: %s :: %s :: %s :: %s" % (messages, flags, mode, uid)


    if uid:
      if not messages.last or messages.last > self.getUIDNext():
         messages.last = self.getUIDNext() -1
    else:
      if not messages.last or messages.last > self.getMessageCount():
        messages.last = self.getMessageCount()

    #print "Message.last: ", messages.last

    # seq => flasgs.str
    result = {}

    for id in messages:
        #print "ID: ", id
      seq = None
      msg_uid = None
      if uid:
        msg_uid = id
        seq = self.getSeqForUid(id)
      else:
        msg_uid = self.seqlist[id - 1][0]
        seq = id
      #print "message uid: %d    seq: %d   isuid: %d" %( msg_uid, seq, uid)
      key = "%s:mailboxes:%s:mail:%s:flags" % (self.user, self.folder, msg_uid)

      #print "Flags %s" % flags
      if mode == REMOVE_FLAGS:
        if flags:
            self.conn.srem(key, *flags)
      elif mode == SET_FLAGS:
        pipe = self.conn.pipeline(transaction=True)
        pipe.multi()
        pipe.delete(key)
        if flags:
            pipe.sadd(key, *flags)
        pipe.execute()
      elif mode == ADD_FLAGS:
        if flags:
            self.conn.sadd(key, *flags)

      stored_flags = self.conn.smembers(key)
      self.correctDeletedList(stored_flags, seq)

      self.conn.publish("%s:mailboxes:%s:channel" % (self.user, self.folder),
              "flags %d" % (msg_uid))

      result[seq] = stored_flags
      return result

  def correctDeletedList(self, flags, seq):
      #account for deleted flag modifications
      #print "Stored flags: " , flags
      if "\\Deleted" in flags:
          #print "DELETED"
        self.deleted_seqs.append(seq)
      else:
          #print " NOT DELETED"
        try:
            self.deleted_seqs.remove(seq)
        except Exception:
            pass
            #print "caught and ignored this, because its not even a problem..."

  def expunge(self):
      deleted_seqs = []
      seq = self.get_next_deleted_seq()
      while seq:
        deleted_seqs.append(seq)
        self.expunge_helper(self.seqlist[seq - 1][0])
        seq = self.get_next_deleted_seq()

      uidlist_key = "%s:mailboxes:%s:uidlist" % (self.user, self.folder)
      self.seqlist = ([(int(x), []) for x in
        self.conn.zrange(uidlist_key, 0, -1)])

      return deleted_seqs

  def get_next_deleted_seq(self):
    if len(self.deleted_seqs):
        return self.deleted_seqs.pop()
    return None

  def expunge_helper(self, uid):
    uidlist_key = "%s:mailboxes:%s:uidlist" % (self.user, self.folder)
    flags_key = "%s:mailboxes:%s:mail:%s:flags" % (self.user, self.folder, uid)
    glob_key = "%s:mailboxes:%s:mail:%s:*" % (self.user,self.folder, uid)
    
    if not self.conn.zrem(uidlist_key, uid):
        return
    keys = self.conn.keys(glob_key)
    keys.remove(flags_key)
    #print "DELETING: ", keys
    self.conn.delete(*keys)
    # decrement count
    count_key = "%s:mailboxes:%s:count" % (self.user, self.folder)
    self.conn.decr(count_key)
    stored_flags = self.conn.smembers(flags_key)
    # set expire on flags_key so that it gets cleanedup after the time
    # the connection timeout is hit. So, either the session times out,
    # or the client updates its view.
    # TODO: if someone else updates this key the expiration is removed.
    #       so keys may accumulate. If this turns out to be an issue, fix it.
    self.conn.expire(flags_key, 3600) 

    
  def destroy(self):
    raise imap4.MailboxException("Not implemented")
    
class CloudFSImapMessage(object):
  implements(imap4.IMessage)
  
  def __init__(self, user, mailbox, uid, pool, flags):
    #print "MESSAGE: %s" % info
    self.user = user
    self.folder = mailbox
    self.uid = uid
    self.session_flags = flags
    self.conn = redis.Redis(connection_pool=pool)
    
  def getUID(self):
    return self.uid
    
  def getFlags(self):
      #print "session flags: ", self.session_flags
      return self.conn.smembers("%s:mailboxes:%s:mail:%s:flags" % (self.user,
        self.folder, self.uid)).union(self.session_flags)

    
  def getInternalDate(self):
    #print "getInternalDate :: %s" % self.info
    #return rfc822date(time.localtime(self.info['created_at_in_seconds']))
    return self.conn.get("%s:mailboxes:%s:mail:%s:date" % (self.user,
      self.folder, self.uid))
    
  def getHeaders(self, negate, *names):
    headerdict = {}
    lowernames = [name.lower() for name in names]
    headers = self.conn.smembers("%s:mailboxes:%s:mail:%s:headers" % (self.user,
      self.folder, self.uid))
    for i in map(lambda s: s.lower(), headers):
      thisheader = self.conn.get("%s:mailboxes:%s:mail:%s:header:%s" %
          (self.user, self.folder, self.uid, i))
      if i in lowernames and not negate:
          headerdict[i] = thisheader
      if i not in lowernames and negate:
          headerdict[i] = thisheader
    #print "getHeaders(): ", lowernames, headerdict
    return headerdict
    
  def getBodyFile(self):
    body = self.conn.get("%s:mailboxes:%s:mail:%s:body" % (self.user,
      self.folder, self.uid))
    headerstart = body.find('\n\n')
    txt = body[headerstart+2:]
    return StringIO(txt)
    
  def getSize(self):
    return self.conn.get("%s:mailboxes:%s:mail:%s:size" % (self.user,
      self.folder, self.uid))
    
  def isMultipart(self):
    return False
    
  def getSubPart(self, part):
      print "SUBPART:: %s" % part
      if part is 0:
          return self
      else:
          raise IndexError




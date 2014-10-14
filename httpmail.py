from twisted.web.client import Agent
from twisted.web.server import NOT_DONE_YET
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from twisted.internet import reactor
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.protocol import Protocol
from zope.interface import implements
import json

class BodyFetcher(Protocol):
  def __init__(self, d):
    self.cb = d
    self.data = ""

  def dataReceived(self, bytes):
    self.data += bytes

  def connectionLost(self, reason):
    self.cb.callback(self.data)

class BodyProducer(object):
  implements(IBodyProducer)

  def __init__(self, body):
      self.body = body
      self.length = len(body)

  @inlineCallbacks
  def startProducing(self, consumer):
      yield consumer.write(self.body)

  def pauseProducing(self):
      pass

  def stopProducing(self):
      pass

class HTTPMail:

  def __init__(self, uri):
    self.uri = uri
    self.agent = Agent(reactor)

  def _connectionFailure(self):
    pass

  def getMailbox(self, mailbox):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}".format(self.uri, mailbox),
        None, None)
    body = Deferred()
    body.addCallback(lambda res: json.loads(res))
    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))

    d.addCallback(resp_callback)
    return body
  

  def getTags(self, mailbox):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}/tags/".format(self.uri, mailbox),
        None, None)
    body = Deferred()
    body.addCallback(lambda res: json.loads(res))
    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))

    d.addCallback(resp_callback)
    return body

  def getTag(self, mailbox, tag):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}/tags/{2}".format(self.uri, mailbox, tag),
        None, None)
    body = Deferred()
    body.addCallback(lambda res: json.loads(res))
    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))

    d.addCallback(resp_callback)
    return body

  def newTag(self, mailbox, tag):
    d = self.agent.request('PUT', 
        "{0}/mailboxes/{1}/tags/{2}".format(self.uri, mailbox, tag),
        None, None)
    return d

  def getMessageTags(self, mailbox, uid):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}/messages/{2}/tags".format(self.uri, mailbox, uid),
        None, None)
    body = Deferred()
    body.addCallback(lambda res: json.loads(res))
    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))

    d.addCallback(resp_callback)
    return body

  def setMessageTag(self, mailbox, uid, tag):
    d = self.agent.request('PUT', 
        "{0}/mailboxes/{1}/messages/{2}/tags/{3}".format(self.uri, mailbox, uid, tag),
        None, None)
    return d

  def deleteMessageTag(self, mailbox, uid, tag):
    d = self.agent.request('DELETE', 
        "{0}/mailboxes/{1}/messages/{2}/tags/{3}".format(self.uri, mailbox, uid, tag),
        None, None)
    return d

  def getMessageFlags(self, mailbox, uid):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}/messages/{2}/flags/".format(self.uri, mailbox, uid),
        None, None)
    body = Deferred()
    body.addCallback(lambda res: json.loads(res))
    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))

    d.addCallback(resp_callback)
    return body

  def setMessageFlag(self, mailbox, uid, flag):
    d = self.agent.request('PUT', 
        "{0}/mailboxes/{1}/messages/{2}/flags/{3}".format(self.uri, mailbox, uid, flag),
        None, None)
    return d

  def deleteMessageFlag(self, mailbox, uid, flag):
    d = self.agent.request('DELETE', 
        "{0}/mailboxes/{1}/messages/{2}/flags/{3}".format(self.uri, mailbox, uid, flag),
        None, None)
    return d

  def deleteTag(self, mailbox, tag):
    d = self.agent.request('DELETE', 
        "{0}/mailboxes/{1}/tag/{2}".format(self.uri, mailbox, tag),
        None, None)
    return d

  def getMessages(self, mailbox):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}/messages/".format(self.uri, mailbox),
        None, None)
    body = Deferred()
    body.addCallback(lambda res: json.loads(res))
    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))
    d.addCallback(resp_callback)
    return body

  def headTag(self, mailbox, dir):
    d = self.agent.request('HEAD', 
        "{0}/mailboxes/{1}/tags/{2}".format(self.uri, mailbox, dir),
        None, None)
    def resp_callback(response):
        unread = int(response.headers.getRawHeaders("x-unread-count")[0])
        total = int(response.headers.getRawHeaders("x-total-count")[0])
        return {"unread": unread, "total": total}
    d.addCallback(resp_callback)
    return d

  def postMessage(self, mailbox, tag, msg):
    prod = BodyProducer(msg)
    d = self.agent.request('POST', 
        "{0}/mailboxes/{1}/tags/{2}".format(self.uri, mailbox, tag),
        Headers({"Content-type": ["message/rfc822"]}),
        prod)
    def resp_callback(resp):
        if resp.code != 201:
            return False
        else:
            return True
    d.addCallback(resp_callback)
    return d

  def deleteMessage(self, mailbox, uid):
    d = self.agent.request('DELETE', 
        "{0}/mailboxes/{1}/messages/{2}".format(self.uri, mailbox, msg),
        None, None)
    return d

  def getMessageMetadata(self, mailbox, msg):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}/messages/{2}/meta".format(self.uri, mailbox, msg),
        None, None)
    body = Deferred()
    body.addCallback(lambda res: json.loads(res))

    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))

    d.addCallback(resp_callback)
    return body

  def getMessage(self, mailbox, msg):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}/messages/{2}".format(self.uri, mailbox, msg),
        Headers({"Accept": ["message/rfc822"]}), 
        None)
    body = Deferred()

    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))

    d.addCallback(resp_callback)
    return body

if __name__  == "__main__":
  c = HTTPMail('http://localhost:5000')
  def cb(x):
    print x
  def msgcb(x):
    c.getMessage('a', x[0]).addCallback(cb)

  c.getMessages('a').addCallback(cb)
  c.headTag('a', 'INBOX').addCallback(cb)
  c.getMessageMetadata('a', '4937e864-94e6-4bfc-89fe-185a9a7b30d5').addCallback(cb)
  c.getMessageTags('a', '4937e864-94e6-4bfc-89fe-185a9a7b30d5').addCallback(cb)
  c.getMessageFlags('a', '4937e864-94e6-4bfc-89fe-185a9a7b30d5').addCallback(cb)

  reactor.run()

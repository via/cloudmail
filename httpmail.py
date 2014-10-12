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

  def getDirectories(self, mailbox):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}/directories".format(self.uri, mailbox),
        None, None)
    body = Deferred()
    body.addCallback(lambda res: json.loads(res))
    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))

    d.addCallback(resp_callback)
    return body

  def newDirectory(self, mailbox, dir):
    d = self.agent.request('PUT', 
        "{0}/mailboxes/{1}/directories/{2}".format(self.uri, mailbox, dir),
        None, None)
    return d

  def deleteDirectory(self, mailbox, dir):
    d = self.agent.request('DELETE', 
        "{0}/mailboxes/{1}/directories/{2}".format(self.uri, mailbox, dir),
        None, None)
    return d

  def getDirectoryMessages(self, mailbox, dir):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}/directories/{2}".format(self.uri, mailbox, dir),
        None, None)
    body = Deferred()
    body.addCallback(lambda res: json.loads(res))
    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))
    d.addCallback(resp_callback)
    return body

  def headDirectory(self, mailbox, dir):
    d = self.agent.request('HEAD', 
        "{0}/mailboxes/{1}/directories/{2}".format(self.uri, mailbox, dir),
        None, None)
    def resp_callback(response):
        unread = int(response.headers.getRawHeaders("x-unread-count")[0])
        total = int(response.headers.getRawHeaders("x-total-count")[0])
        return {"unread": unread, "total": total}
    d.addCallback(resp_callback)
    return d

  def postMessage(self, mailbox, dir, msg):
    prod = BodyProducer(msg)
    d = self.agent.request('POST', 
        "{0}/mailboxes/{1}/directories/{2}".format(self.uri, mailbox, dir),
        Headers({"Content-type": ["message/rfc822"]}),
        prod)
    def resp_callback(resp):
        if resp.code != 201:
            return False
        else:
            return True
    d.addCallback(resp_callback)
    return d

  def getMessage(self, mailbox, msg):
    d = self.agent.request('GET', 
        "{0}/mailboxes/{1}/messages/{2}".format(self.uri, mailbox, msg),
        None, None)
    body = Deferred()

    def resp_callback(response):
      response.deliverBody(BodyFetcher(body))

    d.addCallback(resp_callback)
    return body

if __name__  == "__main__":
  c = HTTPMail('http://localhost:3000')
  def cb(x):
    print x
  def msgcb(x):
    c.getMessage('via', x[0]).addCallback(cb)

  c.getDirectoryMessages('via', 'INBOX').addCallback(msgcb)
  c.headDirectory('via', 'INBOX').addCallback(cb)
  msg = open('test').read()
  c.postMessage('via', 'INBOX', msg)

  c.headDirectory('via', 'INBOX').addCallback(cb)
  reactor.run()

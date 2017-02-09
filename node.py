import simpy
import hashlib
import random


K = 20
SIZE = 2**K
SUCCESSORS = 10

STABILIZE_INT = 1
FIX_FINGER_INT = 4
UPDATE_SUCC_INT = 1

LOG = False

logfile = open('log_chord.log', 'a+')

hosts = {} # { address: Node }


# Calculate distance between node ID a and b
def distance(a, b):
    if a <= b:
        return b - a
    return 2**K + b - a


def displayAll():
    for h in hosts.values():
        h.display()


def allocFun(addr, replica=0, scheme='finger'):
    baseHash = hash(hashlib.sha1(str(addr)).digest())
    if replica == 0:
        return baseHash  % SIZE
    return (baseHash + 2*2**replica) % SIZE


def inrange(c, a, b):
        a = a % SIZE
        b = b % SIZE
        c = c % SIZE

        if a < b:
                return a <= c and c < b
        return a <= c or c < b


def nextId(id):
    return (id + 1) % SIZE


def checkConsistency(totalHosts):
    if not hosts:
        print 'No hosts, consistent'
        return True
    start = hosts[hosts.keys()[0]]
    curr = start
    numHosts = 0
    while curr and curr.predecessor:
        predAddr = curr.predecessor.address
        if predAddr in hosts:
            curr = hosts[predAddr]
            numHosts += 1
        else:
            print 'Invalid pred of node: ', curr.address, predAddr
            return False

        if curr.address == start.address:
            if numHosts == totalHosts:
                #print 'Consistent'
                return True
            else:
                print 'numHosts/expected: ', numHosts, totalHosts
                return False

    return False
    

class Remote:

    msgProbeKey = 0
    msgMakeRep = 0
    msgReqMakeOwner = 0

    def __init__(self, address):
        self.address = address
        self.id = allocFun(self.address)

    
    def rpc(self, cmd, args=None):
        if cmd is 'probe_key':
            Remote.msgProbeKey += 1
        if cmd is 'make_replica':
            Remote.msgMakeRep += 1
        if cmd is 'request_make_owner':
            Remote.msgReqMakeOwner += 1

        if self.address in hosts:
            return hosts[self.address].command(cmd, args)
        else:
            return None


    def __str__(self):
        return 'Remote(%s, %s)' % (str(self.id), str(self.address))

    
    def __repr__(self):
        return self.__str__()


class Node:

    addrCounter = 0
    env = simpy.Environment()

    @staticmethod
    def reset():
        Node.addrCounter = 0
        Node.env = simpy.Environment()

    def __init__(self, bootstrap=None):
        self.stabilize = Node.env.process(self.stabilizeFun())
        self.updateSucc = Node.env.process(self.updateSuccFun())
        self.fixFinger = Node.env.process(self.fixFingerFun())

        self.address = Node.addrCounter
        assert self.address not in hosts
        hosts[self.address] = self
        Node.addrCounter += 1
        self.id = allocFun(self.address)

        self.data = {}
        self.replica = []
        self.replicaCopy = {}

        self.predecessor = None
        self.successors = [None] * SUCCESSORS
        self.fingers = [None] * K

        self.running = True
        self.join(bootstrap)


    def display(self):
        print '\n# id, address: ', self.id, self.address
        print '    pred: ', self.predecessor
        print '    succ: ', self.successors
        print '    fingers', self.fingers
        print '    data:', self.data


    def log(self, info):
        msg = str(self.id) + ' : ' + info
        # print msg
        if logfile and LOG:
            logfile.write(msg + '\n')


    def join(self, bootstrap):
        if bootstrap is None:
            #first node
            self.fingers[0] = Remote(self.address)
        else:
            r = Remote(bootstrap)
            self.fingers[0] = r.rpc('find_successor_of', self.id)
            
        self.successors[0] = self.fingers[0]
    

    def findSuccessorOf(self, id):
        self.log('findSuccessorOf: ' + str(id))
        if self.predecessor and \
                inrange(id, nextId(self.predecessor.id), nextId(self.id)):
            return Remote(self.address)
        else:
            predId = self.findPredecessor(id)
            return predId.rpc('successor')


    def findPredecessor(self, id):
        self.log('findPredecessor')
        if self.successors[0].id == self.id:
            return Remote(self.address)
        node = Remote(self.address)

        while True:
            succ = node.rpc('successor')
            # ??
            if succ is None:
                break

            if inrange(id, nextId(node.id), nextId(succ.id)):
                break
            node = node.rpc('closest_preceding_finger', id)
        return node


    def closestPrecedingFinger(self, id):
        self.log('closestPrecedingFinger')
        for r in reversed(self.successors + self.fingers):
            if r != None and inrange(r.id, nextId(self.id), id) and r.rpc('ping'):
                return r
        return Remote(self.address)


    def notify(self, remote):
        # remote notifies me it is my predecessor
        self.log('notify')
        if self.predecessor is None or \
                not self.predecessor.rpc('ping') or \
                inrange(remote.id, nextId(self.predecessor.id), self.id):
            self.predecessor = remote


    def command(self, cmd, args):
        if cmd == 'successor':
            return self.successor()
        elif cmd == 'get_successors':
            return [ x for x in self.successors[:-1] ]
        elif cmd == 'predecessor':
            return self.predecessor
        elif cmd == 'find_successor_of':
            return self.findSuccessorOf(args)
        elif cmd == 'closest_preceding_finger':
            return self.closestPrecedingFinger(args)
        elif cmd == 'notify':
            self.notify(args)
        elif cmd == 'ping':
            return True
        else:
            print 'Unknown command: ' + cmd


    def successor(self):
        for r in [self.fingers[0]] + self.successors:
            if r is not None and r.rpc('ping'):
                self.fingers[0] = r
                return r
        print 'Error: no successor found, ', self.id
        for r in self.fingers:
            if r is not None and r.rpc('ping'):
                self.join(r.address)
                return r

    def shutdown(self):
        self.running = False
        hosts.pop(self.address, None)


    def stabilizeFun(self):
        clock = 0
        while self.running:
            # stabilize
            self.log('stabilize')
            succ = self.successor()
            if succ != None and succ.id != self.fingers[0].id:
                self.fingers[0] = succ


            predOfSucc = succ.rpc('predecessor')
            if predOfSucc != None and predOfSucc.rpc('ping') and \
                    inrange(predOfSucc.id, nextId(self.id), succ.id) and \
                    nextId(self.id) != succ.id:
                self.fingers[0] = predOfSucc

            self.successor().rpc('notify', Remote(self.address))

            yield Node.env.timeout(STABILIZE_INT)

    def updateSuccFun(self):
        while self.running:
            # update successors
            self.log('update successors')
            succ = self.successor()
            if succ.id != self.id:
                self.successors[0] = succ
                succ_list = succ.rpc('get_successors')
                if succ_list:
                    for i, s in enumerate(succ_list):
                        self.successors[i + 1] = s
            else:
                # alone in the ring
                self.successors = [ succ ] * SUCCESSORS

            yield Node.env.timeout(UPDATE_SUCC_INT)


    def fixFingerFun(self):
        while self.running:
            # fix fingers
            self.log('fix fingers')
            i = random.randrange(1, K)
            self.fingers[i] = self.findSuccessorOf((self.id + 2**i) % SIZE)
            
            yield Node.env.timeout(FIX_FINGER_INT)


if __name__=='__main__':
    n1 = Node()
    for i in range(256):
        Node(n1.address)
    Node.env.run(until=200)

    #displayAll()

    time = 200
    while not checkConsistency(257):
        time += 100
        Node.env.run(until=time)
        print time

    print 'Converged', time


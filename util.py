from replica import *

def runUntilStable(numNodes=len(hosts)):
    start = Node.env.now
    time = Node.env.now + 100
    while not checkConsistency(numNodes):
        Node.env.run(until=time)
        time += 100
        if time > start + 500:
            print 'Error runUntilStable exceeds 500 ticks'
            return

    Node.env.run(until=Node.env.now+100)


def makeChord(numNodes):
    Node.reset()
    hosts.clear()
    n1 = Node()
    for i in range(numNodes-1):
        Node(n1.address)


def makeReplicationChord(numNodes):
    Node.reset()
    hosts.clear()
    n1 = ReplicationNode()
    for i in range(numNodes-1):
        ReplicationNode(n1.address)


def randNode():
    i = randrange(len(hosts))
    return hosts.values()[i]


def crash(address):
    for a in address:
        hosts[a].shutdown()


def crashRand(count):
    for a in sample(hosts, count):
        hosts[a].shutdown()


def chrunAndQuery(churnRate=0.2, query=30):
    pass


if __name__=='__main__':

    makeReplicationChord(200)

    runUntilStable(200)

    #print randNode().address

    #crash([20])
    #crashRand(5)
    #print checkConsistency(len(hosts))
    #print Remote(hosts[9].address).rpc('find_successor_of', 7959737)

    for e in range(1000):
        hosts[0].store(e, e)

    runUntilStable(len(hosts))
    
    Node.env.run(until=Node.env.now+200)

    for i in range(3):
        crashRand(5)
        runUntilStable(len(hosts))

    #env.run(until=1000)

    for e in range(1000):
        if hosts[0].lookup(e) == None:
            print e, 'not found'
            break


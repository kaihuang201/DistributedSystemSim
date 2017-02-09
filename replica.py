from node import *
from time import time
from random import seed, randrange, sample, random, sample


class ReplicationNode(Node):

    MAX_PROBE_COUNT = 10
    REPLICA_COUNT = 5

    OWNER_MAINTEMANCE_INT = 50
    REPLICA_MAINTEMANCE_INT = 40

    REPLICA_TIMEOUT_MIN = 250
    REPLICA_TIMEOUT_MAX = 250

    LOOKUP_BIN_PROBE = False

    probe_tot = 0
    maint_count = 0

    lookup_tot = 0
    lookup_count = 0

    def __init__(self, bootstrap=None):
        Node.__init__(self, bootstrap)

        self.procOwner = Node.env.process(self.runOwnerMaint())
        self.procReplica = Node.env.process(self.runReplicaMaint())


    def store(self, key, value):
        idKey = allocFun(key, 0)
        remote = self.findSuccessorOf(idKey)
        remote.rpc('request_make_owner', (key, value))


    def lookup(self, key):
        allAddr = [allocFun(key, m) for m in range(ReplicationNode.MAX_PROBE_COUNT)]
        while len(allAddr) > 0:
            lookup_tot += 1

            i = randrange(0, len(allAddr))

            idKey = allAddr.pop(i)
            remote = self.findSuccessorOf(idKey)
            if remote == None:
                continue
            retval = remote.rpc('lookup', key)
            if retval != None:
                return retval

        lookup_count += 1

        return None


    def runOwnerMaint(self):

        while self.running:
            for k in self.data:
                if self.data[k][2]:
                    self.ownerMaintenance(k)
            
            yield Node.env.timeout(ReplicationNode.OWNER_MAINTEMANCE_INT)

        
    def runReplicaMaint(self):

        while self.running:
            for k in self.data:
                if not self.data[k][2]:
                    self.replicaMaintenance(k)
            
            yield Node.env.timeout(ReplicationNode.REPLICA_MAINTEMANCE_INT)


    def ownerMaintenance(self, key):

        idReplicaValidated = set([self.id])
        value = self.data[key][0]
        cmake_rep = 0
        m = 0
        for m in range(1, ReplicationNode.MAX_PROBE_COUNT + 1):
            idReplica = allocFun(key, m)
            remote = self.findSuccessorOf(idReplica)

            if remote and remote.id not in idReplicaValidated:
                #if not remote.rpc('probe_key', key):
                remote.rpc('make_replica', (key, value))
                cmake_rep += 1

                idReplicaValidated.add(remote.id)

            if len(idReplicaValidated) >= ReplicationNode.REPLICA_COUNT:
                #print idReplicaValidated, ReplicationNode.REPLICA_COUNT
                break
        
        # print m, cmake_rep, idReplicaValidated, ReplicationNode.MAX_PROBE_COUNT
        ReplicationNode.probe_tot += m
        ReplicationNode.maint_count += 1


    # self.data = {key : (value, timestanp_timeout, isOwner)}
    def replicaMaintenance(self, key):
        value, timeout, isOwner = self.data[key]
        if Node.env.now > timeout:
            # haven't got ping from owner for a long time
            idOwner = allocFun(key)
            remoteOwner = self.findSuccessorOf(idOwner)
            if remoteOwner != None:
                if self.id == remoteOwner.id:
                    # new owner is self, mark key as owned
                    self.requestMakeOwner(key, value)
                else:
                    remoteOwner.rpc('request_make_owner', (key, value))
                    self.data[key][1] = Node.env.now + ReplicationNode.REPLICA_TIMEOUT_MIN + (ReplicationNode.REPLICA_TIMEOUT_MAX - ReplicationNode.REPLICA_TIMEOUT_MIN) * random()


    def requestMakeOwner(self, key, value):
        self.data[key] = [value, 0, True]
        self.ownerMaintenance(key)


    def makeReplica(self, key, value):
        timeout = ReplicationNode.REPLICA_TIMEOUT_MIN + (ReplicationNode.REPLICA_TIMEOUT_MAX - ReplicationNode.REPLICA_TIMEOUT_MIN) * random()
        self.data[key] = [value, Node.env.now + timeout, False]


    def probeKey(self, key):
        timeout = ReplicationNode.REPLICA_TIMEOUT_MIN + (ReplicationNode.REPLICA_TIMEOUT_MAX - ReplicationNode.REPLICA_TIMEOUT_MIN) * random()

        if key in self.data:
            self.data[key] = [self.data[key][0], Node.env.now + timeout, False]
            return True
        else:
            return False


    def command(self, cmd, args):
        if cmd == 'request_make_owner':
            key, value = args
            return self.requestMakeOwner(key, value)

        elif cmd == 'probe_key':
            return self.probeKey(args)

        elif cmd == 'make_replica':
            key, value = args
            return self.makeReplica(key, value)

        elif cmd == 'lookup':
            if args in self.data:
                return self.data[args][0]

            return None

        return Node.command(self, cmd, args)




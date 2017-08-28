package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

// import "bytes"
// import "encoding/gob"

const (
	follower = iota
	candidate
	leader
)

const (
	unknown = iota
	voteRpc
	appendRpc
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
	term       int
	command    interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	Cond       *sync.Cond
	event       int
	role        int
	/*persistent state*/
	currentTerm   int
	votedFor      int
	log           []*Entry

	/*volatile state on all servers*/
	commitIndex   int
	lastApplied   int

	/*volatile state on leaders*/
	nextIndex[]   int
	matchIndex[]  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.role == leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type AppendEntriesArgs struct {
	Term           int
	LeaderId       int
	PrevLogIndex   int
	PrevLogTerm    int
	Entries        []*Entry
	LeaderCommit   int
}

type AppendEntriesreply struct {
	Term      int
	Success   bool
}

func (rf *raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesreply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		//reply.Term = rf.currentTerm
	} 

	if e, ok := log[args.PrevLogIndex]; ok {
		if e.term != args.PrevLogTerm {
			reply.Success = false
			//reply.Term = rf.currentTerm
		}
	}


	/*if success*/
	rf.event = appendRpc
	rf.Cond.Broadcast()
	rf.mu.Unlock()
	return nil
}

func (rf *raft)sendAppendEntries(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term           int
	CandidateId    int
	LastLogIndex   int
	LastLogTerm    int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term          int
	VoteGranted   int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else if (rf.votedFor == nil || rf.votedFor == args.CandidateId) 
				&& (args.LastLogIndex == && args.LastLogTerm == ) {
		reply.VoteGranted = true
	}
	reply.Term = rf.currentTerm
	rf.event = voteRpc
	rf.Cond.Broadcast()
	rf.mu.Unlock()
	return nil
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.Cond = sync.NewCond(&rf.mu)
	rf.event = unknown
	rf.commitIndex = 0
	rf.lastApplied = 0
	
	go func(applyCh chan ApplyMsg) {
		for {
			state := follower
			d1 := rand.Int(150) + 150
			elected := false
			var event int
			rpcCh := make(chan int)
			go checkRpc(rf, rpcCh)

			/*start a new election*/
			for !elected {
				expir := make(chan bool)	
				votech := make(chan bool)

				switch state {
					case follower:
						go startNTimer(d1, expire)

					case candidate:
						rf.mu.Lock()
						rf.currentTerm++
						rf.votedFor = me
						rf.mu.Unlock()
						go startVoteForSelf(rf, ch)
						go startNTimer(d1, expire)
				}

			DONE:
				select {
					case <-expire:
						if rf.state == follower {
							rf.state = candidate
							elected = false
						} else if rf.state == candidate {
							elected = false
						}
					case <-rpcCh:
						if rf.state == follower {
							elected = true
						} else if rf.state == candidate {
							rf.state = follower
							elected = true
						}
					case res = <-votech:
						if res {
							rf.state = leader
							elected = true
						} else {
							fmt.Println("received less than half vote")
							goto DONE
						}
				}
				state = rf.state
			}

			/*Election finished*/
			/*apply the entries to SM asynchronized*/
			go func() {
				for {
					rf.mu.Lock()
					if rf.commitIndex > rf.lastApplied {
						rf.lastApplied++
					} else {
						for rf.event != appendRpc {
							rf.Cond.Wait()
						}
					}
					rf.mu.Unlock()
				}
			}

			if rf.state == leader {
				cond := sync.NewCond(&rf.mu)
				for {
					/*1.Stop others from voting by sending them empty AppendEntries*/
					func() {	
						for i, _ := range rf.peers {
							if i == rf.me {
								continue
							}
							rf.nextIndex[i] = len(rf.log)+1
							rf.matchIndex = 0
						}
						sendAppendEntriesToAll(rf, &Entry{})
					}()

					/*2.accepting entries from client and send them to other servers, should be async*/
					go func() {
						for msg := range applyCh {
							rf.mu.Lock()
							rf.lastApplied = msg.Index
							newEntry = new(Entry)
							newEntry.term = rf.currentTerm
							newEntry.command = msg.command
							rf.log = append(rf.log, newEntry)
							cond.Broadcast()
							rf.mu.Unlock()
							ch1<-true
						}
					}()

					/*send new coming entry to all servers, try again when it fails*/
					for {
						for i, _ := range rf.peers {
							if i == rf.me {
								continue
							}
							rf.mu.Lock()
							if len(log) >= rf.nextIndex[i] {
								index := rf.nextIndex[i]
								res := sendAppendEntriesToEach(rf, index)
								if res {
									rf.nextIndex[i] = len(log) + 1
									rf.matchIndex
								} else {
									rf.nextIndex[i]--
								}
							} else {

							}
							rf.mu.Unlock()
						}
					}
				}
			}

			if rf.state == follower {
				//TODO:
			}
		}
	}(applyCh)
	return rf
}

func sendAppendEntriesToEach(rf *raft, serverid int) bool{
	rf.mu.Lock()
	args := new(AppendEntriesArgs)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.
	args.PrevLogTerm = 
	args.Entries = en
	args.LeaderCommit = rf.commitIndex
	reply := new(AppendEntriesreply)
	rf.mu.Unlock()
	return sendAppendEntries(serverid, args, reply)
}

func startVoteForSelf(rf *Raft, elected chan bool) {
	mutex := sync.Mutex
	votedSum := 0
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if (i == rf.me) {
			continue
		}

		wg.Add(1)
		request := new(RequestVoteArgs)
		rf.mu.Lock()
		request.Term = rf.currentTerm
		request.CandidateId = me
		request.LastLogIndex = len(rf.log)
		request.LastLogTerm = rf.log.term
		rf.mu.Unlock()

		reply := new(RequestVoteReply)
		go func(){
			defer wg.Done()
			ok := rf.sendRequestVote(i, request, reply)
			if ok {
				if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.mu.Unlock
				}
				if reply.VoteGranted {
					mutex.Lock()
					votedSum++
					mutex.Unlock()
				}
			}  else {
				fmt.Println("refused in voting")
			}

			mutex.Lock()
			if votedSum > len(peers)/2 {
				elected<-true
			}
			mutex.Unlock()
		}()
	}
	wg.Wait()
	elected<-false
}

func checkRpc(rf *Raft, c chan int) {
	for {
		rf.mu.Lock()
		for rf.event==unknown {
			rf.Cond.Wait()
		}

		if rf.event != unknown {
			c <- rf.event
			rf.event = unknown
		}
		rf.mu.Unlock()
	}
}

func startNTimer(n int, c chan bool) {
	time.Sleep(time.Duration(n) * time.Millisecond)
	c<-true
}
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
import "math/rand"
import "fmt"
import "time"
import "reflect"

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
	Term       int
	Command    interface{}
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
	timer      *time.Timer
	event       int
	state       int
	servers     int
	stateChange chan bool
	rpcCh       chan int
	expire      chan bool
	voteCh      chan bool
	/*persistent state*/
	currentTerm   int
	votedFor      *int
	log           []Entry

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
	if rf.state == leader {
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
	Entries        []Entry
	LeaderCommit   int
}

type AppendEntriesreply struct {
	Term      int
	Success   bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesreply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.rpcCh<-appendRpc

	reply.Success = true
	//fmt.Println("got append rpc args.Term",args.Term,"at server",rf.me,"currentTerm",rf.currentTerm)
	if args.Term < rf.currentTerm {
		fmt.Println("request term less than receiver term, refused")
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.state = follower
	}

	/*heartbeat rpc carries no entries*/
	if len(args.Entries) == 0 {
		fmt.Println("got heartbeat at server ", rf.me)
	} else if args.PrevLogIndex > 0 {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			t := args.PrevLogIndex-1
			rf.log = rf.log[0:t]
			reply.Success = false
			return
		} else {
			for i:=0;i<len(args.Entries);i++ {
				rf.log = append(rf.log, args.Entries[i])
			}
			reply.Success = true
		}
	}
	
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = func(a,b int) int{
			if a < b {
				return a
			}
			return b
		}(args.LeaderCommit, len(rf.log)-1)
	}

	return
}

func (rf *Raft)sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesreply) bool{
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
	VoteGranted   bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//fmt.Println("got vote request at server id: ", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.state = follower
	}
	
	granted := false
	if rf.votedFor == nil {
		granted = true
	} else if *rf.votedFor == args.CandidateId {
		granted = true
	}
	if !granted {
		reply.VoteGranted = false
		return
	}

	if args.LastLogIndex != len(rf.log) {
		granted = false
	} else {
		if len(rf.log) == 0 {
			if args.LastLogTerm != 0 {
				granted = false
			}
		} else if args.LastLogTerm != rf.log[len(rf.log)-1].Term {
			granted = false
		}
	}
	
	if !granted {
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	rf.rpcCh<-voteRpc
	return
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
	fmt.Println("start cmd ", command)
	mcommand := reflect.ValueOf(command)
	if rf.state != leader {
		return index, term, !isLeader
	}

	/*here comes the leader*/
	fmt.Println("leader found!")
	newEntry := new(Entry)
	newEntry.Term = rf.currentTerm
	newEntry.Command = mcommand
	rf.mu.Lock()
	rf.log = append(rf.log, *newEntry)
	rf.Cond.Broadcast()
	rf.mu.Unlock()
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
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.event = unknown
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.servers = len(peers)
	rf.nextIndex = make([]int, rf.servers)
	rf.matchIndex = make([]int, rf.servers)
	rf.log = append(rf.log, Entry{0,0})

	rf.stateChange = make(chan bool)
	rf.rpcCh = make(chan int)
	rf.expire = make(chan bool)
	rf.voteCh = make(chan bool)

	go func(applyCh chan ApplyMsg) {
		for {
			rf.state = follower
			timer := rand.Intn(150) + 200

			/*default action for every server*/
			go rf.handleLogEntries()

			go rf.handlesignals()
			/*real raft life*/
			rf.Run(timer)
		}
	}(applyCh)
	return rf
}

func (rf *Raft) handleLogEntries() {
	for {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
		} else {
			for rf.commitIndex <= rf.lastApplied {
				rf.Cond.Wait()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) handlesignals() {
	for {
		select {
			case <-rf.expire:
				fmt.Println("time out: ", rf.me)
				rf.handleTimerExpire()
			case <-rf.rpcCh:
				fmt.Println("got rpc changes: ", rf.me)
				rf.handleRpcCall()
			case res := <-rf.voteCh:
				rf.handleVoteRes(res)
		}
	}
}

func (rf *Raft) handleTimerExpire() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == follower || rf.state == candidate {
		rf.state = candidate
		rf.stateChange<-true
	}
}

func (rf *Raft) handleRpcCall() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == follower || rf.state == candidate {
		rf.stopNTimer()
		rf.state = follower
		rf.stateChange<-true
	}
}

func (rf *Raft) handleVoteRes(res bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("vote res: ", res)
	if res {
		rf.state = leader
		rf.stopNTimer()
		rf.stateChange<-true
	} 
	rf.votedFor = nil
}

func (rf *Raft) Run(timer int) {
	for {
		switch rf.state {
			case follower:
				fmt.Println("being a follower: ", rf.me)
				go rf.startNTimer(timer)

			case candidate:
				fmt.Println("being a candidate: ", rf.me)
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = &rf.me
				rf.mu.Unlock()
				go rf.startVoteForSelf()
				go rf.startNTimer(timer)

			case leader:
				fmt.Println("being a leader: ", rf.me)
				rf.initLeader()
				go rf.sendingHeartBeat()
		}
		<-rf.stateChange
	}
}

func (rf *Raft) initLeader() {
	//fmt.Println("initLeader, currentTerm: ", rf.currentTerm)
	for i, _ := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) sendingHeartBeat() {
	state := leader
	for state == leader {
		time.Sleep(time.Duration(20) * time.Millisecond)
		rf.mu.Lock()
		state = rf.state
		rf.mu.Unlock()	
		if state == leader {
			fmt.Println("sending heart beat from ", rf.me,"currentTerm", rf.currentTerm)
			rf.sendAppendEntriesToAll(nil)
		} 
	}
}

func (rf *Raft) sendAppendEntriesToAll(e *Entry){
	success := 0
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(serverid int) {
			defer wg.Done()
			res := false
			rf.mu.Lock()
			args := new(AppendEntriesArgs)
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[serverid]-1
			args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			args.Entries = make([]Entry, 0)
			if e != nil {
				args.Entries = append(args.Entries, *e)
			}
			args.LeaderCommit = rf.commitIndex
			reply := new(AppendEntriesreply)
			rf.mu.Unlock()

			for {
				res = rf.sendAppendEntries(serverid, args, reply)
				fmt.Println("rpc res: ",res)
				if res {
					if reply.Success {
						//fmt.Println("yangtao sending successfully")
						success++
						break
					} else {
						fmt.Println("yangtao reply fail")
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.mu.Lock()
							rf.state = follower
							rf.stateChange<-true
							rf.mu.Unlock()

							break
						} else if args.PrevLogIndex > 0{
							args.PrevLogIndex--
							args.Entries = rf.log[args.PrevLogIndex:rf.nextIndex[serverid]-1]
						} else if args.PrevLogIndex == 0{
							break
						}
						
					}
				} else {
					fmt.Println("sending rpc fails to ", serverid)
					break
				}
			}
		}(i)
	}
	wg.Wait()
	/*if success == rf.servers {
		return true
	}
	return false*/
}

func (rf *Raft) startVoteForSelf() {
	fmt.Println("start to vote")
	var mutex sync.Mutex
	votedSum := 0
	//happened := false
	var wg sync.WaitGroup
	for i, _ := range rf.peers {
		if (i == rf.me) {
			continue
		}

		wg.Add(1)
		go func(serverid int){
			defer wg.Done()
			rf.mu.Lock()
			request := new(RequestVoteArgs)
			request.Term = rf.currentTerm
			request.CandidateId = rf.me
			request.LastLogIndex = len(rf.log)
			request.LastLogTerm = rf.log[len(rf.log)-1].Term
			reply := new(RequestVoteReply)
			rf.mu.Unlock()
			ok := rf.sendRequestVote(serverid, request, reply)
			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.mu.Lock()
					rf.state = follower
					rf.stateChange<-true
					rf.mu.Unlock()
				}
				if reply.VoteGranted {
					mutex.Lock()
					votedSum++
					if votedSum == len(rf.peers)/2 {
						rf.voteCh<-true
					}
					mutex.Unlock()
				}
			} else {
				fmt.Println("refused in voting")
			}

			/*mutex.Lock()
			if votedSum == len(rf.peers)/2 && !happened {
				happened = true
				elected<-true
			}
			mutex.Unlock()*/
		}(i)
	}
	wg.Wait()
	if votedSum < len(rf.peers)/2 {
		rf.voteCh<-false
	}
}

func (rf *Raft) stopNTimer() {
	if rf.timer != nil {
		//fmt.Println("timer stopped")
		if !rf.timer.Stop() {
			<- rf.timer.C
		}
		rf.timer = nil
	}
}

func (rf *Raft) startNTimer(n int) {
	rf.timer = time.NewTimer(time.Duration(n) * time.Millisecond)
	<-rf.timer.C
	fmt.Println("times up")
	rf.expire<-true
}
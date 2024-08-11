package raft

import "time"

func (rf *Raft) startReplication(term int) bool {
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)
		if !ok {
			LOG(rf.me, rf.currentTerm, DError, "AppendEntries for %d, lost or error", peer)
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm < reply.Term {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		return false
	}
	rf.electionStart = time.Now()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
		go replicateToPeer(i, args)
	}
	return true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	if term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	if rf.currentTerm < term {
		rf.becomeFollowerLocked(term)
	}
	reply.Success = true
	reply.Term = term
	rf.electionStart = time.Now()
}

func (rf *Raft) replicationTicker(term int) {
	for {
		ok := rf.startReplication(term)
		if !ok {
			return
		}
		time.Sleep(100 * time.Duration(time.Millisecond))
	}
}

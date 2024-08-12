package raft

import "time"

const (
	replicateInterval time.Duration = 70 * time.Millisecond
)

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
			LOG(rf.me, rf.currentTerm, DApply, "receive a reply %v", reply)
			rf.becomeFollowerLocked(reply.Term)
			return
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.contextLostLocked(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLeader, "Leader[T%d] -> %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}
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
	reply.Term = rf.currentTerm
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log", args.LeaderId)
		reply.Success = false
		return
	}
	LOG(rf.me, rf.currentTerm, DApply, "receive a AppendEntries %v", args)
	if rf.currentTerm <= term {
		rf.becomeFollowerLocked(term)
	}
	reply.Success = true
	rf.resetElectionTimerLocked()
}

func (rf *Raft) replicationTicker(term int) {
	for !rf.killed() {
		ok := rf.startReplication(term)
		if !ok {
			return
		}
		time.Sleep(replicateInterval)
	}
}

package raft

import (
	"math/rand"
	"time"
)

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond
	electionTimeoutMax time.Duration = 400 * time.Millisecond
)

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

func (rf *Raft) isElectionTimeoutLocked() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := args.Term
	reply.Term = rf.currentTerm
	LOG(rf.me, rf.currentTerm, DLog, "receive a RequestVote %v", args)
	if term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.becomeFollowerLocked(args.Term)
	}
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Reject, Already voted S%d", args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
		return
	}
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.resetElectionTimerLocked()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d", args.CandidateId)
	// Your code here (PartA, PartB).
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	LOG(rf.me, rf.currentTerm, DLog, "%s send RequestVote %v to %d", rf.role, args, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection(term int) {

	voteSum := 1
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DError, "RequestVote for %d, lost or error", peer)
			return
		}
		if rf.currentTerm < reply.Term {
			rf.becomeFollowerLocked(reply.Term)
			return
		}
		if rf.contextLostLocked(Candidate, args.Term) {
			return
		}
		LOG(rf.me, rf.currentTerm, DLog, "%s receive voteReply from %d %v", rf.role, peer, reply)
		if reply.VoteGranted {
			voteSum += 1
		}
		if voteSum > len(rf.peers)/2 {
			rf.becomeLeaderLocked()
			go rf.replicationTicker(rf.currentTerm)
		}
	}
	rf.mu.Lock()

	defer rf.mu.Unlock()
	if rf.contextLostLocked(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DLog, "%d term %d", rf.currentTerm, term)
		return
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
		go askVoteFromPeer(i, args)
	}
}

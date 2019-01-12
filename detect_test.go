package deadlocks

import (
	"sync"
	"testing"
	"time"
)

type testResolver struct {
	lock    sync.Mutex
	current int
}

func (r *testResolver) GoroutineID() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.current
}

func (r *testResolver) setGoroutineID(id int) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.current = id
}

func TestDeadlockDetection(t *testing.T) {
	FatalMode = false
	type op struct {
		goroutine   int
		lockNr      int
		instruction int
	}
	const (
		lock = iota
		unlock
		rlock
		runlock
		await_lock
		await_rlock
		wait
	)
	type seq = []op
	tests := []struct {
		name     string
		sequence []op
		deadlock bool
	}{
		{"single_lock", seq{op{1, 1, lock}}, false},
		{"single_rlock", seq{op{1, 1, lock}}, false},

		{"single_unlock", seq{op{1, 1, unlock}}, true},
		{"single_runlock", seq{op{1, 1, runlock}}, true},

		{"double_lock_1", seq{op{1, 1, lock}, op{1, 1, lock}}, true},
		{"double_lock_2", seq{op{1, 1, lock}, op{2, 1, await_lock}}, false},

		{"double_rlock_1", seq{op{1, 1, rlock}, op{1, 1, rlock}}, false},
		{"double_rlock_2", seq{op{1, 1, rlock}, op{2, 1, rlock}}, false},

		{"lock_unlock_1", seq{op{1, 1, lock}, op{1, 1, unlock}}, false},
		{"lock_unlock_2", seq{op{1, 1, lock}, op{2, 1, unlock}}, true},

		{"double_unlock_1", seq{op{1, 1, lock}, op{1, 1, unlock}, op{1, 1, unlock}}, true},
		{"double_unlock_2", seq{op{1, 1, lock}, op{1, 1, unlock}, op{2, 1, unlock}}, true},

		{"rlock_runlock_1", seq{op{1, 1, rlock}, op{1, 1, runlock}}, false},
		{"rlock_runlock_2", seq{op{1, 1, rlock}, op{2, 1, runlock}}, true},

		{"lock_rlock_runlock", seq{op{1, 1, lock}, op{2, 1, await_rlock}, op{1, 1, runlock}}, true},
		{"lock_lock_runlock", seq{op{1, 1, lock}, op{2, 1, await_lock}, op{1, 1, runlock}}, true},

		{"rlock_lock_1", seq{op{1, 1, rlock}, op{1, 1, lock}}, true},
		{"rlock_lock_2", seq{op{1, 1, rlock}, op{2, 1, await_lock}}, false},

		{"rlock_rlock_unlock_1", seq{op{1, 1, rlock}, op{2, 1, rlock}, op{1, 1, unlock}}, true},
		{"rlock_rlock_unlock_2", seq{op{1, 1, rlock}, op{2, 1, rlock}, op{2, 1, unlock}}, true},

		{"rlock_lock_unlock", seq{op{1, 1, rlock}, op{2, 1, await_lock}, op{1, 1, unlock}}, true},

		{"rlock_unlock_1", seq{op{1, 1, rlock}, op{2, 1, unlock}}, true},
		{"rlock_unlock_2", seq{op{1, 1, rlock}, op{1, 1, unlock}}, true},
		{"lock_runlock_1", seq{op{1, 1, lock}, op{2, 1, runlock}}, true},
		{"lock_runlock_2", seq{op{1, 1, lock}, op{1, 1, runlock}}, true},

		{"lock_rlock_unlock", seq{op{1, 1, lock}, op{2, 1, await_rlock}, op{1, 1, unlock}}, false},
		{"rlock_lock_runlock", seq{op{1, 1, rlock}, op{2, 1, await_lock}, op{1, 1, runlock}}, false},

		{"rlock_lock_runlock_unlock", seq{
			op{1, 1, rlock}, op{2, 1, await_lock}, op{1, 1, runlock}, op{2, 1, unlock},
		}, false},
		{"lock_rlock_unlock_runlock", seq{
			op{1, 1, lock}, op{2, 1, await_rlock}, op{1, 1, unlock}, op{2, 1, runlock},
		}, false},

		{"lock_rlock_rlock_unlock_runlock_runlock_lock", seq{
			op{1, 1, lock}, op{2, 1, await_rlock}, op{3, 1, await_rlock},
			op{1, 1, unlock}, op{0, 0, wait}, op{2, 1, runlock}, op{3, 1, runlock},
			op{4, 1, lock},
		}, false},

		{"multiple_locks_1", seq{op{1, 1, lock}, op{1, 2, lock}}, false},
		{"multiple_locks_2", seq{op{1, 1, lock}, op{1, 2, lock}, op{1, 3, lock}}, false},
		{"multiple_locks_3", seq{op{1, 1, lock}, op{1, 2, lock}, op{2, 1, await_lock}}, false},
		{"multiple_locks_4", seq{
			op{1, 1, lock}, op{1, 2, lock},
			op{2, 1, await_lock}, op{2, 2, await_lock},
		}, false},

		{"multiple_rlocks_1", seq{op{1, 1, rlock}, op{1, 2, rlock}}, false},
		{"multiple_rlocks_2", seq{op{1, 1, rlock}, op{1, 2, rlock}, op{1, 3, rlock}}, false},
		{"multiple_rlocks_3", seq{op{1, 1, rlock}, op{1, 2, rlock}, op{2, 1, rlock}}, false},
		{"multiple_rlocks_4", seq{op{1, 1, rlock}, op{1, 2, rlock}, op{2, 1, rlock}, op{2, 2, rlock}}, false},

		{"mixed_locks_1", seq{op{1, 1, lock}, op{1, 2, rlock}}, false},
		{"mixed_locks_2", seq{op{1, 1, lock}, op{1, 2, rlock}, op{1, 3, lock}}, false},
		{"mixed_locks_3", seq{op{1, 1, lock}, op{1, 2, rlock}, op{2, 1, await_lock}}, false},
		{"mixed_locks_4", seq{
			op{1, 1, lock}, op{1, 2, rlock},
			op{2, 1, await_rlock}, op{2, 2, await_lock},
		}, false},

		{"lock_lock_lock_lock_deadlock", seq{
			op{1, 1, lock}, op{2, 2, lock},
			op{1, 2, await_lock}, op{2, 1, lock},
		}, true},

		/*
					------>->-------              ------>---->----
					|              |			  |              |
			   want(1, 2w) -> have(1, 1r) -> want(2, 1w) -> have(2, 2w)
			           |              |              |              |
					   ^			  ----->---->-----              v
					   |			                                |
					   ------<---------<---------<---------<---------
		*/
		{"rlock_lock_lock_lock_deadlock_1", seq{
			op{1, 1, rlock}, op{2, 2, lock},
			op{2, 1, await_lock}, op{1, 2, lock},
		}, true},

		/*
					------>->-------              ------>---->----
					|              |			  |              |
			   want(2, 1w) -> have(2, 2w) -> want(1, 2w) -> have(1, 1r)
			           |              |              |              |
					   ^			  ----->---->-----              v
					   |			                                |
					   ------<---------<---------<---------<---------
		*/
		{"rlock_lock_lock_lock_deadlock_2", seq{
			op{1, 1, rlock}, op{2, 2, lock},
			op{1, 2, await_lock}, op{2, 1, lock},
		}, true},

		/*
					------>->-------              ------>---->----
					|              |			  |              |
			   want(1, 2r) -> have(1, 1r) -> want(2, 1w) -> have(2, 2w)
			           |              |              |              |
					   ^			  ----->---->-----              v
					   |			                                |
					   ------<---------<---------<---------<---------
		*/
		{"rlock_lock_lock_rlock_deadlock_1", seq{
			op{1, 1, rlock}, op{2, 2, lock},
			op{2, 1, await_lock}, op{1, 2, rlock},
		}, true},

		{"rlock_lock_lock_rlock_deadlock_2", seq{
			op{1, 1, rlock}, op{2, 2, lock},
			op{1, 2, await_lock}, op{2, 1, rlock},
		}, false},

		{"rlock_lock_rlock_rlock_1", seq{
			op{1, 1, rlock}, op{2, 2, lock},
			op{2, 1, rlock}, op{1, 2, await_rlock},
		}, false},
		{"rlock_lock_rlock_rlock_2", seq{
			op{1, 1, rlock}, op{2, 2, lock},
			op{1, 2, await_rlock}, op{2, 1, rlock},
		}, false},

		{"cycle_deadlock_1", seq{
			op{1, 1, lock}, op{2, 2, lock}, op{3, 3, lock},
			op{1, 2, await_lock}, op{2, 3, await_lock},
			op{3, 1, lock},
		}, true},
		{"cycle_deadlock_2", seq{
			op{1, 1, rlock}, op{2, 2, rlock}, op{3, 3, lock},
			op{1, 2, await_lock}, op{2, 3, await_rlock},
			op{3, 1, lock},
		}, true},
		{"cycle_deadlock_3", seq{
			op{1, 1, rlock}, op{2, 2, lock}, op{3, 3, rlock},
			op{1, 2, await_rlock}, op{2, 3, await_lock},
			op{3, 1, lock},
		}, true},

		{"cycle_1", seq{
			op{1, 1, lock}, op{2, 2, lock}, op{3, 3, rlock},
			op{1, 2, await_lock}, op{2, 3, rlock},
			op{3, 1, await_lock},
		}, false},
		{"cycle_2", seq{
			op{1, 1, lock}, op{2, 2, lock}, op{3, 3, rlock},
			op{1, 2, await_lock}, op{3, 1, await_lock},
			op{2, 3, rlock},
		}, false},
		{"cycle_3", seq{
			op{1, 1, rlock}, op{2, 2, lock}, op{3, 3, lock},
			op{2, 1, rlock},
			op{3, 2, await_lock}, op{1, 3, await_lock},
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			func() {
				n := 0
				for _, op := range tt.sequence {
					if op.instruction == wait {
						continue
					}
					if op.lockNr <= 0 {
						panic("bad lock nr <= 0 in test spec")
					}
					if op.lockNr > n {
						n = op.lockNr
					}
				}
				globalLock.Lock()
				for k := range goroutineMap {
					delete(goroutineMap, k)
				}
				globalLock.Unlock()
				locks := make([]*sentinel, n)
				resolver := &testResolver{}
				for i := 0; i < len(locks); i++ {
					locks[i] = Sentinel("lock-%d", i+1).(*sentinel)
					locks[i].resolver = resolver
				}
				for pos, op := range tt.sequence {
					if op.instruction == wait {
						time.Sleep(2 * time.Millisecond)
						continue
					}
					ready := make(chan interface{})
					go func(pos int, ready chan interface{}) {
						defer func() {
							ready <- recover()
						}()
						l := locks[op.lockNr-1]
						resolver.setGoroutineID(op.goroutine)
						<-ready
						switch op.instruction {
						case lock:
							l.Lock()
						case unlock:
							l.Unlock()
						case rlock:
							l.RLock()
						case runlock:
							l.RUnlock()
						case await_lock:
							go func() {
								defer func() {
									err := recover()
									if err != nil {
										ready <- err
									} else {
										ready <- false
									}
								}()
								l.Lock()
							}()
							time.Sleep(1 * time.Millisecond)
						case await_rlock:
							go func() {
								defer func() {
									err := recover()
									if err != nil {
										ready <- err
									} else {
										ready <- false
									}
								}()
								l.RLock()
							}()
							time.Sleep(1 * time.Millisecond)
						}
						ready <- nil
					}(pos, ready)
					ready <- true
					select {
					case err := <-ready:
						if err == nil {
							if pos == len(tt.sequence)-1 && tt.deadlock {
								t.Fatalf("failed to detect deadlock")
							} else {
								continue
							}
						}
						if err == false {
							t.Fatalf("failed to complete expected deadlock sequence: "+
								"unexpectedly NOT blocking at pos %d\n\nsequence: %v\n\n",
								pos, tt.sequence)
						}
						if pos != len(tt.sequence)-1 {
							t.Fatalf("failed to complete expected deadlock sequence: "+
								"at pos %d not %d\n\n%v\n\nsequence: %v\n\n",
								pos, len(tt.sequence)-1, err, tt.sequence)
						}
						// ok, test passes
					case <-time.After(2 * time.Millisecond):
						if op.instruction != await_lock && op.instruction != await_rlock {
							t.Fatalf("failed to complete expected deadlock sequence: "+
								"unexpectedly blocking at pos %d\n\nsequence: %v\n\n",
								pos, tt.sequence)
						}
						if tt.deadlock {
							t.Fatalf("failed to detect deadlock: test timed out at pos %d\n\n"+
								"sequence: %v\n\n", pos, tt.sequence)
						}
					}
				}
			}()
		})
	}
}

package deadlocks

import (
	"bytes"
	"context"
	"fmt"
	stdlog "log"
	"runtime"
	"sync"
	"time"

	"github.com/modern-go/gls"
	"go.uber.org/zap"

	"github.com/jsnathan/log"
)

// Mutex is a drop-in replacement for a sync.Mutex.
//   Call Sentinel(<name-of-lock>) to initialize it.
type Mutex interface {
	Lock()
	Unlock()
}

// RWMutex is a drop-in replacement for a sync.RWMutex.
//   Call Sentinel(<name-of-lock>) to initialize it.
type RWMutex interface {
	Mutex

	RLock()
	RUnlock()
}

type GoroutineIDResolver interface {
	GoroutineID() int
}

// represents a lock
//   implements Mutex
//   implements RWMutex
type sentinel struct {
	id       string
	statlock sync.Mutex
	mutex    *sync.RWMutex
	resolver GoroutineIDResolver
	record   *sentinel
	readers  map[int]bool
	waiters  map[int]bool
	writer   int
	count    int
	stacks   map[int][]byte
}

// used by the cycle detection logic in findCycle
type cycleNode struct {
	want   *sentinel
	have   *sentinel
	g      int
	wantRW bool
	haveRW bool
}

type defaultGoroutineIDResolver struct{}

// Disabled allows turning the deadlock detection logic off temporarily,
//   or for production use.
var Disabled = true

// LoggingEnabled controls whether or not the deadlock detection logic
//   generates logs. Do not use this in production. Default: false
var LoggingEnabled = false

// FatalMode controls whether or not a detected deadlock should shut down
//   the entire process (true), or simply panic (false). Default: true
var FatalMode = true

// FatalModeHandler is an optional callback that can be set, which is invoked
//   before the process is shut down. If the callback returns false,
//     the process is not shut down. It is invoked with the deadlock log message,
//       which contains the complete stack traces of all running goroutines.
//   Note that if the handler returns false, while the process will not exit,
//     the call which triggered the deadlock will still panic.
//       Otherwise we would be deadlocked.
var FatalModeHandler func(string) bool

var bufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 16e3)
	},
}

var globalLock sync.Mutex
var goroutineMap = map[int]map[string]*sentinel{}

var timeouts = map[int]map[string]context.CancelFunc{}
var timeoutsLock sync.Mutex

// Sentinel returns a RWMutex (which also doubles as a normal Mutex).
//  The sentinel mutex is special, because it detects deadlock.
//    This adds some runtime overhead, but often this is negligible.
func Sentinel(lockID string, args ...interface{}) RWMutex {
	s := new(sentinel)
	s.id = fmt.Sprintf(lockID, args...)
	s.mutex = new(sync.RWMutex)
	s.resolver = defaultGoroutineIDResolver{}
	s.waiters = make(map[int]bool)
	s.readers = make(map[int]bool)
	s.stacks = make(map[int][]byte)
	return s
}

func (s *sentinel) Lock() {
	if Disabled {
		s.mutex.Lock()
		return
	}
	g := s.resolver.GoroutineID()
	s.beginLock(g)
	s.mutex.Lock()
	s.completeLock(g)
}

func (s *sentinel) Unlock() {
	if Disabled {
		s.mutex.Unlock()
		return
	}
	g := s.resolver.GoroutineID()

	// todo: merge these two routines
	s.beginUnlock(g)
	s.completeUnlock(g)

	s.mutex.Unlock()
}

func (s *sentinel) RLock() {
	if Disabled {
		s.mutex.RLock()
		return
	}
	g := s.resolver.GoroutineID()
	s.beginReadLock(g)
	s.mutex.RLock()
	s.completeReadLock(g)
}

func (s *sentinel) RUnlock() {
	if Disabled {
		s.mutex.RUnlock()
		return
	}
	g := s.resolver.GoroutineID()

	// todo: merge these two routines
	s.beginReadUnlock(g)
	s.completeReadUnlock(g)

	s.mutex.RUnlock()
}

func (s *sentinel) beginReadLock(g int) {
	id := s.id

	globalLock.Lock()
	defer globalLock.Unlock()
	if LoggingEnabled {
		log.Log.Debugw("Starting read-lock",
			"func", "deadlocks.beginReadLock",
			"goroutine-id", g,
			"lock-id", id,
			zap.Stack("stack"),
		)
	}
	s.pushStack(g)
	if s.writer == g {
		s.deadlockDetected("Deadlock Detected: Attempt to ACQUIRE readlock %s WHILE HOLDING writelock", s.id)
	}
	if _, ok := s.readers[g]; ok {
		s.deadlockDetected("Possible Deadlock Detected: Attempt to ACQUIRE SECOND readlock for %s", s.id)
	}
	if s.writer != 0 || len(s.readers) > 0 {
		s.findCycle(cycleNode{
			want:   s,
			g:      g,
			wantRW: false,
		})
	}
	s.waiters[g] = false
}

func (s *sentinel) completeReadLock(g int) {
	globalLock.Lock()
	defer globalLock.Unlock()

	if LoggingEnabled {
		log.Log.Debugw("Completing read-lock",
			"func", "deadlocks.completeReadLock",
			"goroutine-id", g,
			"lock-id", s.id,
			zap.Stack("stack"),
		)
	}
	delete(s.waiters, g)
	s.readers[g] = true
	locks, ok := goroutineMap[g]
	if !ok {
		locks = make(map[string]*sentinel)
		goroutineMap[g] = locks
	}
	locks[s.id] = s
}

func (s *sentinel) beginReadUnlock(g int) {
	globalLock.Lock()
	defer globalLock.Unlock()
	id := s.id

	if LoggingEnabled {
		log.Log.Debugw("Starting read-unlock",
			"func", "deadlocks.beginReadUnlock",
			"goroutine-id", g,
			"lock-id", id,
			zap.Stack("stack"),
		)
	}
	s.popStack(g)
	if _, ok := s.readers[g]; !ok {
		if s.writer == g {
			s.deadlockDetected(
				"Deadlock Detected: Attempt to RELEASE readlock %s WHILE HOLDING writelock", s.id)
		} else if rw, ok := s.waiters[g]; ok {
			lockType := "readlock"
			if rw {
				lockType = "writelock"
			}
			panic(fmt.Sprintf(
				"bad deadlock detection logic (attempt to release readlock while waiting for %s)", lockType))
		} else {
			s.deadlockDetected("Deadlock Detected: Attempt to RELEASE readlock %s WE ARE NOT HOLDING", s.id)
		}
	}
}

func (s *sentinel) completeReadUnlock(g int) {
	globalLock.Lock()
	defer globalLock.Unlock()

	if LoggingEnabled {
		log.Log.Debugw("Completing read-unlock",
			"func", "deadlocks.completeReadUnlock",
			"goroutine-id", g,
			"lock-id", s.id,
			zap.Stack("stack"),
		)
	}
	delete(s.readers, g)
	delete(goroutineMap[g], s.id)
}

func (s *sentinel) beginLock(g int) {
	id := s.id

	globalLock.Lock()
	defer globalLock.Unlock()
	if LoggingEnabled {
		log.Log.Debugw("Starting write-lock",
			"func", "deadlocks.beginLock",
			"goroutine-id", g,
			"lock-id", id,
			zap.Stack("stack"),
		)
	}
	s.pushStack(g)
	if s.writer == g {
		s.deadlockDetected("Deadlock Detected: Attempt to ACQUIRE writelock %s WHILE HOLDING writelock", s.id)
	}
	if _, ok := s.readers[g]; ok {
		s.deadlockDetected("Deadlock Detected: Attempt to ACQUIRE writelock %s WHILE HOLDING readlock", s.id)
	}
	if s.writer != 0 || len(s.readers) > 0 {
		s.findCycle(cycleNode{
			want:   s,
			g:      g,
			wantRW: true,
		})
	}
	s.waiters[g] = true
}

func (s *sentinel) completeLock(g int) {
	globalLock.Lock()
	defer globalLock.Unlock()

	if LoggingEnabled {
		log.Log.Debugw("Completing write-lock",
			"func", "deadlocks.completeLock",
			"goroutine-id", g,
			"lock-id", s.id,
			zap.Stack("stack"),
		)
	}
	delete(s.waiters, g)
	s.writer = g
	locks, ok := goroutineMap[g]
	if !ok {
		locks = make(map[string]*sentinel)
		goroutineMap[g] = locks
	}
	locks[s.id] = s
	s.startTimeout(g)
}

func (s *sentinel) beginUnlock(g int) {
	globalLock.Lock()
	defer globalLock.Unlock()
	id := s.id

	if LoggingEnabled {
		log.Log.Debugw("Starting write-unlock",
			"func", "deadlocks.beginUnlock",
			"goroutine-id", g,
			"lock-id", id,
			zap.Stack("stack"),
		)
	}
	s.popStack(g)
	if s.writer != g {
		if _, ok := s.readers[g]; ok {
			s.deadlockDetected(
				"Deadlock Detected: Attempt to RELEASE writelock %s WHILE HOLDING readlock", s.id)
		} else if rw, ok := s.waiters[g]; ok {
			lockType := "readlock"
			if rw {
				lockType = "writelock"
			}
			panic(fmt.Sprintf(
				"bad deadlock detection logic (attempt to release writelock while waiting for %s)", lockType))
		} else {
			s.deadlockDetected("Deadlock Detected: Attempt to RELEASE writelock %s WE ARE NOT HOLDING", s.id)
		}
	}
	s.stopTimeout(g)
}

func (s *sentinel) completeUnlock(g int) {
	globalLock.Lock()
	defer globalLock.Unlock()

	if LoggingEnabled {
		log.Log.Debugw("Completing write-unlock",
			"func", "deadlocks.completeUnlock",
			"goroutine-id", g,
			"lock-id", s.id,
			zap.Stack("stack"),
		)
	}
	s.writer = 0
	delete(goroutineMap[g], s.id)
}

func (s *sentinel) findCycle(path ...cycleNode) {
	var first = path[0]
	var last = path[len(path)-1]
	for _, rHave := range goroutineMap[last.g] {
		path[len(path)-1].have = rHave
		for g, rw := range rHave.waiters {
			for _, rNext := range goroutineMap[g] {
				if rHave.id == rNext.id {
					continue
				}
				if !rw && rHave.writer != last.g {
					// read locks do not block other read locks
					continue
				}
				nextPath := append(path, cycleNode{
					want:   rHave,
					have:   rNext,
					g:      g,
					wantRW: rw,
					haveRW: rNext.writer == g,
				})
				if first.want.id == rNext.id {
					if first.wantRW || rNext.writer == g {
						cycle := ""
						for _, n := range nextPath {
							if n.have != nil {
								haveType := "readlock"
								if n.haveRW {
									haveType = "writelock "
								}

								cycle += "---\n\n"
								cycle += fmt.Sprintf("<- goroutine %d has %s for %s\n\n",
									n.g, haveType, n.have.id)
								cycle += string(pruneStack(n.have.stacks[n.g])) + "\n"
							}
							wantType := "readlock"
							if n.wantRW {
								wantType = "writelock"
							}
							cycle += fmt.Sprintf("-> goroutine %d wants %s for %s\n\n",
								n.g, wantType, n.want.id)
							cycle += "---\n\n"
						}
						cycle += "\n"
						s.deadlockCycleDetected("Potential Deadlock Cycle Detected:\n\n%s", cycle)
					}
				}
				unique := true
				for _, n := range path {
					if g == n.g {
						unique = false
						break
					}
				}
				if unique {
					s.findCycle(nextPath...)
				}
			}
		}
	}
}

var allStacks = make([]byte, 5e6)

func (s *sentinel) deadlockDetected(msg string, args ...interface{}) {
	fmt.Printf("--- DEADLOCK DETECTED ---\n\n")
	// show all stacks, except the latest (which is the new one)

	var newStack []byte
	for g, stack := range s.stacks {
		_, old := goroutineMap[g][s.id]
		if old {
			_, ro := s.readers[g]
			fmt.Printf("---\n")
			stack = pruneStack(stack)
			if ro {
				fmt.Printf("goroutine %d holds readlock:\n%s\n", g, stack)
			} else if s.writer == g {
				fmt.Printf("goroutine %d holds writelock:\n%s\n", g, stack)
			} else {
				fmt.Printf("ERROR: bad deadlock detection logic (goroutineMap and record out of sync)")
			}
			fmt.Printf("---\n")
		} else {
			newStack = stack
		}
	}
	// show the new stack
	fmt.Printf("NEW:\n%s\n", newStack)
	fmt.Printf("\n--- --- --- ---\n\n")

	msg = fmt.Sprintf(msg, args...)
	if FatalMode {
		fullMsg := fmt.Sprintf("%s\n\n%s\n\n", msg, allStacks[:runtime.Stack(allStacks, true)])
		if FatalModeHandler != nil {
			proceed := FatalModeHandler(fullMsg)
			if !proceed {
				// handler decided not to shutdown the process
				panic(fullMsg)
			}
		}
		stdlog.Fatalln(fullMsg)
	} else {
		panic(msg)
	}
}

func (s *sentinel) deadlockCycleDetected(msg string, args ...interface{}) {
	fmt.Printf("--- DEADLOCK CYCLE DETECTED ---\n\n")
	msg = fmt.Sprintf(msg, args...)
	// note: a panic will not necessarilly shut down the entire process;
	//   it will likely be caught by deferred handler in the server or db stack
	if FatalMode {
		fullMsg := fmt.Sprintf("%s\n\n%s\n\n", msg, allStacks[:runtime.Stack(allStacks, true)])
		if FatalModeHandler != nil {
			proceed := FatalModeHandler(fullMsg)
			if !proceed {
				// handler decided not to shutdown the process
				panic(msg)
			}
		}
		stdlog.Fatalln(fullMsg)
	} else {
		panic(msg)
	}
}

func pruneStack(stack []byte) []byte {
	for i := 0; i < 4; i++ {
		stack = stack[bytes.Index(stack, []byte("\n")):]
	}
	return stack
}

func (s *sentinel) startTimeout(g int) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	timeoutsLock.Lock()
	stack := s.stacks[g]
	t := timeouts[g]
	if t == nil {
		t = make(map[string]context.CancelFunc)
		timeouts[g] = t
	}
	t[s.id] = cancel
	timeoutsLock.Unlock()
	go func() {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				s.deadlockDetected("Deadlock Detected: Write lock held for TOO LONG\n\n%s\n", stack)
			}
		}
	}()
}

func (s *sentinel) stopTimeout(g int) {
	timeoutsLock.Lock()
	t := timeouts[g]
	cancel := t[s.id]
	cancel()
	delete(t, s.id)
	timeoutsLock.Unlock()
}

func (s *sentinel) pushStack(g int) {
	buf := bufPool.Get().([]byte)
	n := runtime.Stack(buf, false)
	s.stacks[g] = buf[:n]

}
func (s *sentinel) popStack(g int) {
	if buf, ok := s.stacks[g]; ok {
		bufPool.Put(buf[:cap(buf)])
		delete(s.stacks, g)
	}
}

func (_ defaultGoroutineIDResolver) GoroutineID() int {
	id := int(gls.GoID())
	if id == 0 {
		panic("found gls.GoID() == 0")
	}
	return id
}

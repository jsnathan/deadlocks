package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
)

const defaultLogLocation = "/tmp/test.log"

func main() {

	var scanner *bufio.Scanner

	if flag.NArg() >= 1 {
		inputFile := flag.Arg(0)
		f, err := os.Open(inputFile)
		if err != nil {
			log.Fatalln("Failed to open log input file", inputFile, "because:", err)
		}
		scanner = bufio.NewScanner(f)
	} else if stat, _ := os.Stdin.Stat(); (stat.Mode() & os.ModeCharDevice) == 0 {
		// pipe input
		scanner = bufio.NewScanner(os.Stdin)
	} else {
		f, err := os.Open(defaultLogLocation)
		if err != nil {
			log.Fatalln("Failed to open default log input file", defaultLogLocation, "because:", err)
		}
		scanner = bufio.NewScanner(f)
	}

	r := regexp.MustCompile(`{"`)

	waitingMap := make(map[int]map[string]bool)
	activeMap := make(map[int]map[string]bool)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		var offset int
		if loc := r.FindStringIndex(line); loc != nil && loc[0] > 0 {
			offset = loc[0]
			line = line[offset:]
		}
		m := map[string]interface{}{}
		err := json.Unmarshal([]byte(line), &m)
		if err != nil {
			continue
		}

		if _, ok := m["lock-id"]; !ok {
			continue
		}

		id := m["lock-id"].(string)
		g := AsInt(m["goroutine-id"])
		op := m["func"].(string)[len("deadlocks."):]
		// fmt.Println(g, op, id)
		switch op {
		case "beginLock":
			m := waitingMap[g]
			if m == nil {
				m = make(map[string]bool)
				waitingMap[g] = m
			}
			m[id] = true
		case "completeLock":
			delete(waitingMap[g], id)
			m := activeMap[g]
			if m == nil {
				m = make(map[string]bool)
				activeMap[g] = m
			}
			m[id] = true
		case "beginReadLock":
			m := waitingMap[g]
			if m == nil {
				m = make(map[string]bool)
				waitingMap[g] = m
			}
			m[id] = false
		case "completeReadLock":
			delete(waitingMap[g], id)
			m := activeMap[g]
			if m == nil {
				m = make(map[string]bool)
				activeMap[g] = m
			}
			m[id] = false
		case "completeUnlock":
			delete(activeMap[g], id)
		case "completeReadUnlock":
			delete(activeMap[g], id)
		}
	}

	fmt.Printf("Waiting:\n")
	waitingLocks := make(map[int][]string)
	waitingLocksIDs := make([]int, 0, 16)
	for g, m := range waitingMap {
		if len(m) == 0 {
			continue
		}
	outer1:
		for id, rw := range m {
			lockType := "readlock"
			if rw {
				lockType = "writelock"
			}
			waitingLocks[g] = append(waitingLocks[g],
				fmt.Sprintf("waiting for a %s on %s\n", lockType, id))
			for _, G := range waitingLocksIDs {
				if G == g {
					continue outer1
				}
			}
			waitingLocksIDs = append(waitingLocksIDs, g)
		}
	}
	sort.Ints(waitingLocksIDs)
	for _, g := range waitingLocksIDs {
		waiters := waitingLocks[g]
		sort.Strings(waiters)
		for _, w := range waiters {
			fmt.Printf("  goroutine %d is %s", g, w)
		}
	}
	fmt.Printf("\n")

	fmt.Printf("Active:\n")
	activeLocks := make(map[string][]string)
	activeLocksIDs := make([]string, 0, 16)
	for g, m := range activeMap {
		if len(m) == 0 {
			continue
		}
	outer2:
		for id, rw := range m {
			lockType := "readlock"
			if rw {
				lockType = "writelock"
			}
			output := fmt.Sprintf("  goroutine %d is holding a %s on %s\n", g, lockType, id)
			for _, w := range waitingLocks[g] {
				output += fmt.Sprintf("    it is also %s", w)
			}
			activeLocks[id] = append(activeLocks[id], output)
			for _, ID := range activeLocksIDs {
				if ID == id {
					continue outer2
				}
			}
			activeLocksIDs = append(activeLocksIDs, id)
		}
	}
	sort.Strings(activeLocksIDs)
	for _, id := range activeLocksIDs {
		fmt.Printf("\n")
		active := activeLocks[id]
		sort.Strings(active)
		for _, a := range active {
			fmt.Printf(a)
		}
	}
	fmt.Printf("\n")
}

func AsInt(in interface{}) int {
	switch n := in.(type) {
	case float64:
		return int(n)
	case uint64:
		return int(n)
	}
	return 0
}

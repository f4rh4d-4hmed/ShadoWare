//go:build !windows

package main

import "syscall"

func isParentDead(ppid int) bool {
	err := syscall.Kill(ppid, 0)
	return err != nil
}

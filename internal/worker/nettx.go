package worker

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// netDevPath is the kernel's per-interface counters for this process's
// network namespace — the Pod's, in Kubernetes.
var netDevPath = "/proc/self/net/dev"

// netTxBytes is the bytes sent over every interface but loopback, or 0 when
// the counters cannot be read (not Linux). The coordinator reads its growth
// as progress while a slow storage delays the acks (#422).
func netTxBytes() int64 {
	f, err := os.Open(netDevPath)
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()
	return parseNetDevTx(bufio.NewScanner(f))
}

// parseNetDevTx sums the transmitted bytes (the ninth counter after the
// interface name) of every interface but lo.
func parseNetDevTx(sc *bufio.Scanner) int64 {
	var total int64
	for sc.Scan() {
		iface, counters, ok := strings.Cut(sc.Text(), ":")
		if !ok || strings.TrimSpace(iface) == "lo" {
			continue
		}
		fields := strings.Fields(counters)
		if len(fields) < 9 {
			continue
		}
		if n, err := strconv.ParseInt(fields[8], 10, 64); err == nil {
			total += n
		}
	}
	return total
}

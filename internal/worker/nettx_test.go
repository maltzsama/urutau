package worker

import (
	"bufio"
	"strings"
	"testing"
)

func TestParseNetDevTxSumsEveryInterfaceButLoopback(t *testing.T) {
	const dev = `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:  999999     100    0    0    0     0          0         0   999999     100    0    0    0     0       0          0
  eth0: 5000000    4000    0    0    0     0          0         0  7340032    3000    0    0    0     0       0          0
  eth1:     100       1    0    0    0     0          0         0      2048       2    0    0    0     0       0          0
`
	if got := parseNetDevTx(bufio.NewScanner(strings.NewReader(dev))); got != 7340032+2048 {
		t.Fatalf("tx = %d, want %d", got, 7340032+2048)
	}
}

package bonjour

import (
	"net"
	"strings"
	"testing"

	"github.com/Formlabs/dns"
)

var querier = &net.UDPAddr{IP: net.ParseIP("10.35.9.68"), Port: 5353}

type sentPacket struct {
	msg dns.Msg
	to  *net.UDPAddr
}

func testEntry(t *testing.T) *ServiceEntry {
	t.Helper()
	entry, err := MakeServiceEntry("Formlabs Fuse 1+ (10W) X", "_formlabs_formule._tcp", "local", 35, "X.local.",
		[]net.IP{net.ParseIP("10.35.15.50"), net.ParseIP("fe80::1")}, []string{"txtv=1", "app=formule"})
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func testServer(t *testing.T) (*Server, *[]sentPacket) {
	t.Helper()
	sent := &[]sentPacket{}
	s := &Server{ttl: 300}
	s.send = func(buf []byte, addr *net.UDPAddr) error {
		p := sentPacket{to: addr}
		if err := p.msg.Unpack(buf); err != nil {
			t.Fatal(err)
		}
		*sent = append(*sent, p)
		return nil
	}
	return s, sent
}

// receive packs query as a client would and runs it through the server's receive path
func receive(t *testing.T, s *Server, entry *ServiceEntry, query *dns.Msg, from *net.UDPAddr) {
	t.Helper()
	buf, err := query.Pack()
	if err != nil {
		t.Fatal(err)
	}
	req, err := s.parsePacket(buf, from)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.handleQuery(entry, req); err != nil {
		t.Fatal(err)
	}
}

func question(name string, qtype uint16, unicast bool) dns.Question {
	class := uint16(dns.ClassINET)
	if unicast {
		class |= 1 << 15
	}
	return dns.Question{Name: name, Qtype: qtype, Qclass: class}
}

func countTypes(rrs []dns.RR) map[uint16]int {
	counts := map[uint16]int{}
	for _, rr := range rrs {
		counts[rr.Header().Rrtype]++
	}
	return counts
}

func TestNamesMatchIgnoringCaseAndEscaping(t *testing.T) {
	entry := testEntry(t)
	cases := []struct {
		name  string
		qtype uint16
		want  []uint16
	}{
		{entry.ServiceInstanceName(), dns.TypeSRV, []uint16{dns.TypeSRV, dns.TypeTXT}},
		{strings.ToLower(entry.ServiceInstanceName()), dns.TypeSRV, []uint16{dns.TypeSRV, dns.TypeTXT}},
		{strings.ToUpper(entry.ServiceInstanceName()), dns.TypeTXT, []uint16{dns.TypeSRV, dns.TypeTXT}},
		{strings.ToUpper(entry.HostName), dns.TypeA, []uint16{dns.TypeA, dns.TypeAAAA}},
		{strings.ToUpper(entry.ServiceName()), dns.TypePTR, []uint16{dns.TypePTR}},
	}
	for _, c := range cases {
		s, sent := testServer(t)
		q := new(dns.Msg)
		q.Question = []dns.Question{question(c.name, c.qtype, true)}
		receive(t, s, entry, q, querier)
		if len(*sent) != 1 {
			t.Fatalf("%q: got %d replies, want 1", c.name, len(*sent))
		}
		types := countTypes((*sent)[0].msg.Answer)
		for _, want := range c.want {
			if types[want] == 0 {
				t.Errorf("%q: reply has no %s record", c.name, dns.TypeToString[want])
			}
		}
	}
}

func TestCanonicalName(t *testing.T) {
	cases := map[string]string{
		`Formlabs\ Fuse\ 1+\ \(10W\)\ X._formlabs_formule._tcp.local.`: "formlabs fuse 1+ (10w) x._formlabs_formule._tcp.local.",
		`A\.B\\C\032D.local.`: `a\.b\\c d.local.`,
		`X.LOCAL.`:            "x.local.",
		// a dot inside a label is not a label separator
		`a\.b.local.`:   `a\.b.local.`,
		`a\046b.local.`: `a\.b.local.`,
		// only A to Z fold: \195\137 is the UTF-8 for É, \195\169 for é
		`\195\137.local.`: "\xc3\x89.local.",
		`\195\169.local.`: "\xc3\xa9.local.",
		`\065.local.`:     "a.local.",
	}
	for in, want := range cases {
		if got := canonicalName(in); got != want {
			t.Errorf("canonicalName(%q) = %q, want %q", in, got, want)
		}
	}
}

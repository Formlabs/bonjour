package bonjour

import (
	"net"
	"strings"
	"testing"
	"time"

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

// settle sends every reply that is waiting out a delay
func settle(s *Server) {
	s.sendDue(time.Now().Add(time.Second))
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
		settle(s)
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

func browseWithKnownAnswer(entry *ServiceEntry, instance string, ttl uint32) *dns.Msg {
	q := new(dns.Msg)
	q.Question = []dns.Question{question(entry.ServiceName(), dns.TypePTR, true)}
	q.Answer = []dns.RR{&dns.PTR{
		Hdr: dns.RR_Header{Name: entry.ServiceName(), Rrtype: dns.TypePTR, Class: dns.ClassINET, Ttl: ttl},
		Ptr: instance + "." + entry.ServiceName(),
	}}
	return q
}

func TestKnownAnswerSuppression(t *testing.T) {
	entry := testEntry(t)
	cases := []struct {
		desc      string
		instance  string
		ttl       uint32
		wantReply bool
	}{
		{"unrelated known answer", "Formlabs Form 4 Y", 300, true},
		{"own PTR at full TTL", entry.Instance, 300, false},
		{"own PTR at half TTL", strings.ToUpper(entry.Instance), 150, false},
		{"own PTR below half TTL", entry.Instance, 100, true},
	}
	for _, c := range cases {
		s, sent := testServer(t)
		receive(t, s, entry, browseWithKnownAnswer(entry, c.instance, c.ttl), querier)
		settle(s)
		if got := len(*sent) == 1 && countTypes((*sent)[0].msg.Answer)[dns.TypePTR] == 1; got != c.wantReply {
			t.Errorf("%s: PTR reply sent = %v, want %v", c.desc, got, c.wantReply)
		}
	}
}

// RFC 6762 11: a query sent to the multicast group is on the link whatever its source
// address, so hosts on another subnet of the same link are answered too
func TestSourcesOnOtherSubnetsAreAnswered(t *testing.T) {
	entry := testEntry(t)
	for _, from := range []string{"10.35.9.68", "192.168.1.20", "169.254.10.1", "fe80::2", "2001:db8::2"} {
		s, sent := testServer(t)
		q := new(dns.Msg)
		q.Question = []dns.Question{question(entry.HostName, dns.TypeA, false)}
		receive(t, s, entry, q, &net.UDPAddr{IP: net.ParseIP(from), Port: 5353})
		if len(*sent) != 1 {
			t.Errorf("query from %s: got %d replies, want 1", from, len(*sent))
		}
	}
}

func TestOnlyPlainQueriesAreAnswered(t *testing.T) {
	entry := testEntry(t)
	cases := []struct {
		desc string
		set  func(m *dns.Msg)
	}{
		{"response", func(m *dns.Msg) { m.Response = true }},
		{"non-zero opcode", func(m *dns.Msg) { m.Opcode = dns.OpcodeUpdate }},
		{"non-zero response code", func(m *dns.Msg) { m.Rcode = dns.RcodeServerFailure }},
	}
	for _, c := range cases {
		s, sent := testServer(t)
		m := browseWithKnownAnswer(entry, "Formlabs Form 4 Y", 300)
		c.set(m)
		receive(t, s, entry, m, querier)
		if len(*sent) != 0 {
			t.Errorf("%s: got %d replies, want 0", c.desc, len(*sent))
		}
	}
}

func TestRepliesOthersMayAnswerAreDelayed(t *testing.T) {
	entry := testEntry(t)
	browse := question(entry.ServiceName(), dns.TypePTR, false)
	host := question(entry.HostName, dns.TypeA, false)
	lookup := question(entry.ServiceInstanceName(), dns.TypeSRV, false)
	short := [2]time.Duration{20 * time.Millisecond, 120 * time.Millisecond}
	long := [2]time.Duration{400 * time.Millisecond, 500 * time.Millisecond}
	cases := []struct {
		desc      string
		questions []dns.Question
		truncate  bool
		delay     [2]time.Duration
		unicast   bool
	}{
		{"shared record", []dns.Question{browse}, false, short, false},
		{"shared record, truncated query", []dns.Question{browse}, true, long, false},
		{"shared record, unicast reply", []dns.Question{question(entry.ServiceName(), dns.TypePTR, true)}, false, short, true},
		{"unique records, several questions", []dns.Question{host, lookup}, false, short, false},
		{"unique record, truncated query", []dns.Question{host}, true, long, false},
	}
	for _, c := range cases {
		s, sent := testServer(t)
		q := new(dns.Msg)
		q.Truncated = c.truncate
		q.Question = c.questions
		before := time.Now()
		receive(t, s, entry, q, querier)
		after := time.Now()
		if len(*sent) != 0 || len(s.pending) != 1 || s.pendingTimer == nil {
			t.Fatalf("%s: %d replies sent and %d waiting, want 0 and 1", c.desc, len(*sent), len(s.pending))
		}
		if due := s.pending[0].due; due.Before(before.Add(c.delay[0])) || due.After(after.Add(c.delay[1])) {
			t.Errorf("%s: delay %v, want %v to %v", c.desc, due.Sub(before), c.delay[0], c.delay[1])
		}
		s.sendDue(s.pending[0].due.Add(-time.Millisecond))
		if len(*sent) != 0 {
			t.Fatalf("%s: reply sent before its delay was over", c.desc)
		}
		s.sendDue(s.pending[0].due)
		// the answers to every question of the query share one packet (RFC 6762 6.4)
		if len(*sent) != 1 || len(s.pending) != 0 || s.pendingTimer != nil {
			t.Fatalf("%s: %d replies sent and %d waiting once the delay was over, want 1 and 0", c.desc, len(*sent), len(s.pending))
		}
		if got := (*sent)[0].to != nil; got != c.unicast {
			t.Errorf("%s: reply sent by unicast = %v, want %v", c.desc, got, c.unicast)
		}
		if len(c.questions) == 2 {
			if types := countTypes((*sent)[0].msg.Answer); types[dns.TypeA] == 0 || types[dns.TypeSRV] == 0 {
				t.Errorf("%s: reply holds %v, want the answers to both questions", c.desc, types)
			}
		}
	}
}

func TestLoneUniqueRepliesAreNotDelayed(t *testing.T) {
	entry := testEntry(t)
	for _, q := range []dns.Question{
		question(entry.HostName, dns.TypeA, false),
		question(entry.HostName, dns.TypeA, true),
		question(entry.ServiceInstanceName(), dns.TypeSRV, false),
	} {
		s, sent := testServer(t)
		m := new(dns.Msg)
		m.Question = []dns.Question{q}
		receive(t, s, entry, m, querier)
		if len(*sent) != 1 || len(s.pending) != 0 {
			t.Fatalf("%s: want one immediate reply", q.String())
		}
		// an instance lookup is answered with the instance's records, not the shared PTRs
		if countTypes((*sent)[0].msg.Answer)[dns.TypePTR] != 0 {
			t.Errorf("%s: reply holds a PTR record it was not asked for", q.String())
		}
	}
}

// A truncated query that joins a reply already waiting holds it back until that
// query's known answers have had time to arrive
func TestTruncatedQueryExtendsWaitingReply(t *testing.T) {
	entry := testEntry(t)
	s, _ := testServer(t)
	q := new(dns.Msg)
	q.Question = []dns.Question{question(entry.ServiceName(), dns.TypePTR, false)}
	receive(t, s, entry, q, querier)
	q.Truncated = true
	before := time.Now()
	receive(t, s, entry, q, &net.UDPAddr{IP: net.ParseIP("10.35.9.69"), Port: 5353})
	if len(s.pending) != 1 {
		t.Fatalf("%d replies waiting, want the two queries to share one", len(s.pending))
	}
	if due := s.pending[0].due; due.Before(before.Add(400 * time.Millisecond)) {
		t.Errorf("reply due %v after the truncated query, want at least 400ms", due.Sub(before))
	}
}

func TestContinuationKnownAnswersApplyToDeferredReply(t *testing.T) {
	entry := testEntry(t)
	other := &net.UDPAddr{IP: net.ParseIP("10.35.9.69"), Port: 5353}
	for _, c := range []struct {
		desc      string
		unicast   bool
		alsoAsked *net.UDPAddr
		from      *net.UDPAddr
		wantEmpty bool
	}{
		{"continuation from the querier", false, nil, querier, true},
		{"continuation from the querier, unicast reply", true, nil, querier, true},
		{"packet from another host", false, nil, other, false},
		{"continuation from the querier while another host waits for the same reply", false, other, querier, false},
	} {
		s, sent := testServer(t)
		q := new(dns.Msg)
		q.Truncated = true
		q.Question = []dns.Question{question(entry.ServiceName(), dns.TypePTR, c.unicast)}
		receive(t, s, entry, q, querier)
		if c.alsoAsked != nil {
			receive(t, s, entry, q, c.alsoAsked)
		}
		cont := browseWithKnownAnswer(entry, entry.Instance, 300)
		cont.Question = nil
		receive(t, s, entry, cont, c.from)
		if len(s.pending) != 1 {
			t.Fatalf("%s: %d replies waiting, want 1", c.desc, len(s.pending))
		}
		got := len(s.pending[0].msg.Answer) == 0
		settle(s)
		if got != c.wantEmpty || got != (len(*sent) == 0) {
			t.Errorf("%s: deferred reply emptied = %v with %d replies sent, want %v", c.desc, got, len(*sent), c.wantEmpty)
		}
	}
}

// A record set sent with the cache-flush bit replaces the set its receivers hold, so
// when the querier knows only some of the host's addresses the whole set is sent
func TestUniqueRecordSetsAreSentWhole(t *testing.T) {
	entry, err := MakeServiceEntry("X", "_formlabs_formule._tcp", "local", 35, "X.local.",
		[]net.IP{net.ParseIP("fe80::1"), net.ParseIP("2001:db8::1")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	known := func(ips ...string) []dns.RR {
		var rrs []dns.RR
		for _, ip := range ips {
			rrs = append(rrs, &dns.AAAA{
				Hdr:  dns.RR_Header{Name: entry.HostName, Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 300},
				AAAA: net.ParseIP(ip),
			})
		}
		return rrs
	}
	cases := []struct {
		desc  string
		known []dns.RR
		want  int
	}{
		{"none known", nil, 2},
		{"one of two known", known("fe80::1"), 2},
		{"both known", known("fe80::1", "2001:db8::1"), 0},
	}
	for _, c := range cases {
		s, sent := testServer(t)
		q := new(dns.Msg)
		q.Question = []dns.Question{question(entry.HostName, dns.TypeAAAA, false)}
		q.Answer = c.known
		receive(t, s, entry, q, querier)
		got := 0
		if len(*sent) == 1 {
			got = countTypes((*sent)[0].msg.Answer)[dns.TypeAAAA]
		}
		if got != c.want {
			t.Errorf("%s: reply holds %d AAAA records, want %d", c.desc, got, c.want)
		}
	}
}

func TestCacheFlushOnUniqueRecordsOnly(t *testing.T) {
	entry := testEntry(t)
	for _, name := range []string{entry.HostName, entry.ServiceName(), entry.ServiceInstanceName(), entry.ServiceTypeName()} {
		for _, unicast := range []bool{true, false} {
			s, sent := testServer(t)
			q := new(dns.Msg)
			q.Question = []dns.Question{question(name, dns.TypeANY, unicast)}
			receive(t, s, entry, q, querier)
			settle(s)
			if len(*sent) != 1 {
				t.Fatalf("%s unicast=%v: got %d replies, want 1", name, unicast, len(*sent))
			}
			reply := &(*sent)[0].msg
			for _, rr := range append(reply.Answer, reply.Extra...) {
				shared := rr.Header().Rrtype == dns.TypePTR
				if flush := rr.Header().Class&cacheFlushBit != 0; flush == shared {
					t.Errorf("%s unicast=%v: %s has cache-flush %v", name, unicast, dns.TypeToString[rr.Header().Rrtype], flush)
				}
			}
		}
	}
}

// A legacy reply repeats the ID and question of one query, so it is neither
// delayed nor merged with other replies, even for shared records
func TestLegacyRepliesAreSentAtOnce(t *testing.T) {
	entry := testEntry(t)
	s, sent := testServer(t)
	q := new(dns.Msg)
	q.Id = 1234
	q.Question = []dns.Question{
		question(entry.ServiceName(), dns.TypePTR, false),
		question(entry.HostName, dns.TypeA, false),
	}
	receive(t, s, entry, q, &net.UDPAddr{IP: querier.IP, Port: 54321})
	if len(*sent) != 2 || len(s.pending) != 0 {
		t.Fatalf("%d replies sent and %d waiting, want 2 and 0", len(*sent), len(s.pending))
	}
	for i, p := range *sent {
		if p.to == nil || p.msg.Id != 1234 || len(p.msg.Question) != 1 || p.msg.Question[0].Name != q.Question[i].Name {
			t.Errorf("reply %d: to %v, id %d, questions %v", i, p.to, p.msg.Id, p.msg.Question)
		}
	}
}

func TestResponseHeader(t *testing.T) {
	entry := testEntry(t)
	legacyQuerier := &net.UDPAddr{IP: querier.IP, Port: 54321}
	cases := []struct {
		desc    string
		from    *net.UDPAddr
		unicast bool
		legacy  bool
	}{
		{"QU query", querier, true, false},
		{"QM query", querier, false, false},
		{"legacy unicast query", legacyQuerier, false, true},
	}
	for _, c := range cases {
		s, sent := testServer(t)
		q := new(dns.Msg)
		q.Id = 1234
		q.Question = []dns.Question{question(entry.HostName, dns.TypeA, c.unicast)}
		receive(t, s, entry, q, c.from)
		if len(*sent) != 1 {
			t.Fatalf("%s: got %d replies, want 1", c.desc, len(*sent))
		}
		p := (*sent)[0]
		if !p.msg.Response || !p.msg.Authoritative || p.msg.Opcode != dns.OpcodeQuery {
			t.Errorf("%s: header %+v, want QR and AA set and opcode 0", c.desc, p.msg.MsgHdr)
		}
		if wantUnicast := c.unicast || c.legacy; (p.to != nil) != wantUnicast {
			t.Errorf("%s: sent to %v, want unicast %v", c.desc, p.to, wantUnicast)
		}
		if !c.legacy {
			if p.msg.Id != 0 || len(p.msg.Question) != 0 {
				t.Errorf("%s: id %d and %d questions, want 0 and 0", c.desc, p.msg.Id, len(p.msg.Question))
			}
			continue
		}
		if p.msg.Id != 1234 || len(p.msg.Question) != 1 {
			t.Errorf("%s: id %d and %d questions, want 1234 and 1", c.desc, p.msg.Id, len(p.msg.Question))
		}
		for _, rr := range append(p.msg.Answer, p.msg.Extra...) {
			if rr.Header().Ttl > 10 || rr.Header().Class&cacheFlushBit != 0 {
				t.Errorf("%s: %s", c.desc, rr.String())
			}
		}
	}
}

// The limit follows a record into the additional section: a browse reply does not
// repeat an address multicast a moment ago, and the addresses it does carry count as sent
func TestMulticastRateLimitCoversAdditionalRecords(t *testing.T) {
	entry := testEntry(t)
	host := new(dns.Msg)
	host.Question = []dns.Question{question(entry.HostName, dns.TypeA, false)}
	browse := new(dns.Msg)
	browse.Question = []dns.Question{question(entry.ServiceName(), dns.TypePTR, false)}
	addresses := func(p sentPacket) int {
		all := countTypes(append(p.msg.Answer, p.msg.Extra...))
		return all[dns.TypeA] + all[dns.TypeAAAA]
	}

	s, sent := testServer(t)
	receive(t, s, entry, host, querier)
	receive(t, s, entry, browse, querier)
	settle(s)
	if len(*sent) != 2 || addresses((*sent)[0]) != 2 {
		t.Fatalf("got %d replies, want the addresses and then the browse reply", len(*sent))
	}
	if types := countTypes((*sent)[1].msg.Extra); addresses((*sent)[1]) != 0 || types[dns.TypeSRV] != 1 || types[dns.TypeTXT] != 1 {
		t.Errorf("browse reply after an address reply carries %v, want SRV and TXT only", types)
	}

	s, sent = testServer(t)
	receive(t, s, entry, browse, querier)
	settle(s)
	receive(t, s, entry, host, querier)
	if len(*sent) != 1 || addresses((*sent)[0]) != 2 {
		t.Errorf("got %d replies, want only the browse reply with its addresses", len(*sent))
	}
}

func TestMulticastRateLimit(t *testing.T) {
	entry := testEntry(t)
	s, sent := testServer(t)
	q := new(dns.Msg)
	q.Question = []dns.Question{question(entry.HostName, dns.TypeA, false)}
	receive(t, s, entry, q, querier)
	receive(t, s, entry, q, querier)
	if len(*sent) != 1 {
		t.Fatalf("got %d replies to two queries within a second, want 1", len(*sent))
	}
	for k := range s.lastMulticast {
		s.lastMulticast[k] = time.Now().Add(-time.Second)
	}
	receive(t, s, entry, q, querier)
	if len(*sent) != 2 {
		t.Errorf("got %d replies once a second had passed, want 2", len(*sent))
	}
}

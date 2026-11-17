package bonjour

import (
	"net"
	"slices"
	"strings"
	"sync"
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

// RFC 6762 18: a response has QR and AA set, a zero ID and no question section
func TestResponseHeader(t *testing.T) {
	entry := testEntry(t)
	for _, unicast := range []bool{true, false} {
		s, sent := testServer(t)
		q := new(dns.Msg)
		q.Id = 1234
		q.Question = []dns.Question{question(entry.HostName, dns.TypeA, unicast)}
		receive(t, s, entry, q, querier)
		if len(*sent) != 1 {
			t.Fatalf("unicast %v: got %d replies, want 1", unicast, len(*sent))
		}
		p := (*sent)[0]
		if !p.msg.Response || !p.msg.Authoritative || p.msg.Opcode != dns.OpcodeQuery {
			t.Errorf("unicast %v: header %+v, want QR and AA set and opcode 0", unicast, p.msg.MsgHdr)
		}
		if (p.to != nil) != unicast {
			t.Errorf("unicast %v: sent to %v", unicast, p.to)
		}
		if p.msg.Id != 0 || len(p.msg.Question) != 0 {
			t.Errorf("unicast %v: id %d and %d questions, want 0 and 0", unicast, p.msg.Id, len(p.msg.Question))
		}
	}
}

// accept packs query as a client would and runs it through the path packets take from
// the sockets, where truncated queries are held
func accept(t *testing.T, s *Server, entry *ServiceEntry, query *dns.Msg, from *net.UDPAddr, now time.Time) {
	t.Helper()
	buf, err := query.Pack()
	if err != nil {
		t.Fatal(err)
	}
	req, err := s.parsePacket(buf, from)
	if err != nil {
		t.Fatal(err)
	}
	s.accept(entry, req, now)
}

// A querier that knows more printers than fit in one packet sets the TC bit and lists
// the rest in further packets, which hold no question. Its query is held until those
// have had time to arrive, and answered with all its known answers applied.
func TestTruncatedQueriesWaitForTheirKnownAnswers(t *testing.T) {
	entry := testEntry(t)
	other := &net.UDPAddr{IP: net.ParseIP("10.35.9.69"), Port: 5353}
	known := func(instance string, truncated bool) *dns.Msg {
		m := browseWithKnownAnswer(entry, instance, 300)
		m.Question = nil
		m.Truncated = truncated
		return m
	}
	for _, c := range []struct {
		desc          string
		first         string // instance listed in the query itself
		continuations []*dns.Msg
		contFrom      *net.UDPAddr
		want          int
	}{
		{"no continuation arrives", "Formlabs Form 4 Other", nil, querier, 1},
		{"continuation does not list us", "Formlabs Form 4 Other", []*dns.Msg{known("Formlabs Form 4 Another", false)}, querier, 1},
		{"continuation lists us", "Formlabs Form 4 Other", []*dns.Msg{known(entry.Instance, false)}, querier, 0},
		{"second continuation lists us", "Formlabs Form 4 Other", []*dns.Msg{known("Formlabs Form 4 Another", true), known(entry.Instance, false)}, querier, 0},
		{"query itself lists us", entry.Instance, nil, querier, 0},
		{"another host lists us", "Formlabs Form 4 Other", []*dns.Msg{known(entry.Instance, false)}, other, 1},
	} {
		s, sent := testServer(t)
		now := time.Now()
		q := browseWithKnownAnswer(entry, c.first, 300)
		q.Truncated = true
		accept(t, s, entry, q, querier, now)
		for i, cont := range c.continuations {
			accept(t, s, entry, cont, c.contFrom, now.Add(time.Duration(i+1)*10*time.Millisecond))
		}
		if len(*sent) != 0 || len(s.held) != 1 || s.heldTimer == nil {
			t.Fatalf("%s: %d replies sent and %d queries held before the wait is over, want 0 and 1", c.desc, len(*sent), len(s.held))
		}
		s.answerHeld(entry, now.Add(truncatedWait-time.Millisecond))
		if len(*sent) != 0 {
			t.Fatalf("%s: answered before the wait was over", c.desc)
		}
		s.answerHeld(entry, now.Add(truncatedWait+20*time.Millisecond))
		if len(*sent) != c.want || len(s.held) != 0 || s.heldTimer != nil {
			t.Errorf("%s: %d replies sent and %d queries still held, want %d and 0", c.desc, len(*sent), len(s.held), c.want)
		}
	}
}

// Queries that are not truncated are answered at once, also while another querier's
// truncated query waits, and a querier cannot make its own wait last forever
func TestTruncatedQueryWaitIsBounded(t *testing.T) {
	entry := testEntry(t)
	other := &net.UDPAddr{IP: net.ParseIP("10.35.9.69"), Port: 5353}
	s, sent := testServer(t)
	now := time.Now()
	tc := browseWithKnownAnswer(entry, "Formlabs Form 4 Other", 300)
	tc.Truncated = true
	accept(t, s, entry, tc, querier, now)

	plain := new(dns.Msg)
	plain.Question = []dns.Question{question(entry.ServiceName(), dns.TypePTR, false)}
	accept(t, s, entry, plain, other, now)
	if len(*sent) != 1 || len(s.held) != 1 {
		t.Fatalf("%d replies sent and %d queries held after another host's plain query, want 1 and 1", len(*sent), len(s.held))
	}

	// truncated continuations keep arriving; the answer goes out by the limit all the same
	more := browseWithKnownAnswer(entry, "Formlabs Form 4 Another", 300)
	more.Question = nil
	more.Truncated = true
	for at := now; at.Before(now.Add(3 * time.Second)); at = at.Add(100 * time.Millisecond) {
		accept(t, s, entry, more, querier, at)
		s.answerHeld(entry, at)
	}
	if len(*sent) != 2 || len(s.held) != 0 {
		t.Errorf("%d replies sent and %d queries held after 3 s of truncated continuations, want 2 and 0", len(*sent), len(s.held))
	}

	// a new query from a querier whose query is held ends the wait for the held one
	*sent = nil
	accept(t, s, entry, tc, querier, now)
	accept(t, s, entry, plain, querier, now.Add(50*time.Millisecond))
	if len(*sent) != 2 || len(s.held) != 0 {
		t.Errorf("%d replies sent and %d queries held after a second query from the same host, want 2 and 0", len(*sent), len(s.held))
	}

	// with too many queriers waiting, further truncated queries are answered at once
	*sent = nil
	for i := 0; i < maxHeldQueries+5; i++ {
		accept(t, s, entry, tc, &net.UDPAddr{IP: net.IPv4(10, 35, 10, byte(i)), Port: 5353}, now)
	}
	if len(s.held) != maxHeldQueries || len(*sent) != 5 {
		t.Errorf("%d queries held and %d answered at once, want %d and 5", len(s.held), len(*sent), maxHeldQueries)
	}
}

// The records are announced four times when the server starts and four times more
// when they change, and not again after that
func TestAnnouncementsStop(t *testing.T) {
	entry := testEntry(t)
	const interval = 20 * time.Millisecond
	var mu sync.Mutex
	var sent []dns.Msg
	var times []time.Time
	s := &Server{ttl: 300, announceInterval: interval, initialEntry: entry, shutdownChan: make(chan bool)}
	s.send = func(buf []byte, addr *net.UDPAddr) error {
		var m dns.Msg
		if err := m.Unpack(buf); err != nil {
			t.Error(err)
		}
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, m)
		times = append(times, time.Now())
		return nil
	}
	addresses := func(from, to int) (ips []string) {
		mu.Lock()
		defer mu.Unlock()
		if len(sent) < to {
			t.Fatalf("%d announcements sent, want at least %d", len(sent), to)
		}
		for _, m := range sent[from:to] {
			for _, rr := range m.Extra {
				if a, ok := rr.(*dns.A); ok {
					ips = append(ips, a.A.String())
				}
			}
		}
		return ips
	}
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(sent)
	}

	entries := make(chan *ServiceEntry)
	s.waitGroup.Add(1)
	start := time.Now()
	go s.mainloop(entries)
	defer func() { s.shutdownChan <- true }()

	// the burst ends 8 intervals after the start; nothing follows in the next 17
	time.Sleep(25 * interval)
	if n := count(); n != announceCount {
		t.Fatalf("%d announcements after the first burst, want %d", n, announceCount)
	}
	if ips := addresses(0, 4); !slices.Equal(ips, []string{"10.35.15.50", "10.35.15.50", "10.35.15.50", "10.35.15.50"}) {
		t.Errorf("first burst announced addresses %v", ips)
	}
	mu.Lock()
	for i, want := range []time.Duration{1, 2, 4, 8} {
		if got := times[i].Sub(start); got < want*interval {
			t.Errorf("announcement %d sent after %v, want at least %v", i, got, want*interval)
		}
	}
	mu.Unlock()

	changed := testEntry(t)
	changed.AddrsIPv4 = []net.IP{net.ParseIP("10.35.15.51").To4()}
	entries <- changed
	time.Sleep(25 * interval)
	if n := count(); n != 2*announceCount {
		t.Fatalf("%d announcements after the records changed, want %d", n, 2*announceCount)
	}
	if ips := addresses(4, 8); !slices.Equal(ips, []string{"10.35.15.51", "10.35.15.51", "10.35.15.51", "10.35.15.51"}) {
		t.Errorf("second burst announced addresses %v", ips)
	}

	// a change in the middle of a burst starts the burst over with the new records
	entries <- entry
	time.Sleep(3 * interval)
	during := count()
	entries <- changed
	time.Sleep(25 * interval)
	if n := count(); n != during+announceCount {
		t.Errorf("%d announcements after a change in mid-burst, want %d", n-during, announceCount)
	}
}

// Once the announcements are done the records are repeated at the keep-alive interval,
// and a change of the records starts over with the announcements
func TestKeepAliveAnnouncements(t *testing.T) {
	entry := testEntry(t)
	const interval = 20 * time.Millisecond
	const keepAlive = 10 * interval
	var mu sync.Mutex
	var times []time.Time
	s := &Server{ttl: 300, announceInterval: interval, keepAliveInterval: keepAlive, initialEntry: entry, shutdownChan: make(chan bool)}
	s.send = func(buf []byte, addr *net.UDPAddr) error {
		mu.Lock()
		defer mu.Unlock()
		times = append(times, time.Now())
		return nil
	}
	sent := func() []time.Time {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(times)
	}
	entries := make(chan *ServiceEntry)
	s.waitGroup.Add(1)
	go s.mainloop(entries)
	defer func() { s.shutdownChan <- true }()

	// the announcements end 8 intervals after the start; keep-alives follow 10 intervals apart
	time.Sleep(8*interval + 2*keepAlive + keepAlive/2)
	got := sent()
	if n := len(got) - announceCount; n < 2 || n > 3 {
		t.Fatalf("%d keep-alive announcements in two and a half keep-alive intervals, want 2", n)
	}
	for i := announceCount; i < len(got); i++ {
		if gap := got[i].Sub(got[i-1]); gap < keepAlive {
			t.Errorf("keep-alive %d came %v after the announcement before it, want at least %v", i-announceCount, gap, keepAlive)
		}
	}

	// a change announces afresh: four announcements within 8 intervals, less than one keep-alive
	before := len(sent())
	entries <- entry
	time.Sleep(9 * interval)
	if n := len(sent()) - before; n != announceCount {
		t.Errorf("%d announcements in the 9 intervals after a change, want %d", n, announceCount)
	}
}

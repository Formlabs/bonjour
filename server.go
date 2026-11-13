package bonjour

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Formlabs/dns"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

const cacheFlushBit = 1 << 15

var (
	// Multicast groups used by mDNS
	mdnsGroupIPv4 = net.IPv4(224, 0, 0, 251)
	mdnsGroupIPv6 = net.ParseIP("ff02::fb")

	// mDNS wildcard addresses
	mdnsWildcardAddrIPv4 = &net.UDPAddr{
		IP:   net.ParseIP("224.0.0.0"),
		Port: 5353,
	}
	mdnsWildcardAddrIPv6 = &net.UDPAddr{
		IP:   net.ParseIP("ff02::"),
		Port: 5353,
	}

	// mDNS endpoint addresses
	ipv4Addr = &net.UDPAddr{
		IP:   mdnsGroupIPv4,
		Port: 5353,
	}
	ipv6Addr = &net.UDPAddr{
		IP:   mdnsGroupIPv6,
		Port: 5353,
	}
)

func MakeServiceEntry(instance, service, domain string,
	port int, host string, ips []net.IP, text []string) (*ServiceEntry, error) {
	entry := NewServiceEntry(instance, service, domain)
	entry.Port = port
	entry.Text = text
	entry.HostName = host

	if entry.Instance == "" {
		return nil, fmt.Errorf("Missing service instance name")
	}
	if entry.Service == "" {
		return nil, fmt.Errorf("Missing service name")
	}
	if entry.HostName == "" {
		return nil, fmt.Errorf("Missing host name")
	}
	if entry.Domain == "" {
		entry.Domain = "local"
	}
	if entry.Port == 0 {
		return nil, fmt.Errorf("Missing port")
	}

	if !strings.HasSuffix(trimDot(entry.HostName), entry.Domain) {
		entry.HostName = fmt.Sprintf("%s.%s.", trimDot(entry.HostName), trimDot(entry.Domain))
	}

	for _, ipAddr := range ips {
		if ipAddr == nil {
			return nil, fmt.Errorf("Failed to parse given IP: %v", ipAddr)
		} else if ipv4 := ipAddr.To4(); ipv4 != nil {
			entry.AddrsIPv4 = append(entry.AddrsIPv4, ipv4)
		} else if ipv6 := ipAddr.To16(); ipv6 != nil {
			entry.AddrsIPv6 = append(entry.AddrsIPv6, ipv6)
		} else {
			return nil, fmt.Errorf("The IP is neither IPv4 nor IPv6: %#v", ipAddr)
		}
	}

	entry.HostName = strings.ToLower(entry.HostName)

	return entry, nil
}

// Register a service proxy by given argument. This call will skip the hostname/IP lookup and
// will use the provided values.
func RegisterProxy(initialEntry *ServiceEntry, entries chan *ServiceEntry, iface *net.Interface) (*Server, error) {
	s, err := newServer(initialEntry, iface)
	if err != nil {
		return nil, err
	}

	s.waitGroup.Add(1)
	go s.mainloop(entries)

	return s, nil
}

// Server structure incapsulates both IPv4/IPv6 UDP connections
type Server struct {
	ipv4conn          *net.UDPConn
	ipv6conn          *net.UDPConn
	initialEntry      *ServiceEntry
	shouldShutdown    bool
	shutdownChan      chan bool
	waitGroup         sync.WaitGroup
	shutdownLock      sync.Mutex
	ttl               uint32
	// send writes a packet to addr, or to both multicast groups when addr is nil
	send func(buf []byte, addr *net.UDPAddr) error

	// Replies waiting out their RFC 6762 6 delay, and the timer for the earliest. Owned by mainloop.
	pending      []*pendingReply
	pendingTimer <-chan time.Time

	// When each answer record was last multicast, keyed by its text form. Owned by mainloop.
	lastMulticast map[string]time.Time
}

// Constructs server structure
func newServer(initialEntry *ServiceEntry, iface *net.Interface) (*Server, error) {
	// Create wildcard connections (because :5353 can be already taken by other apps)
	ipv4conn, err := net.ListenUDP("udp4", mdnsWildcardAddrIPv4)
	if err != nil {
		log.Printf("[ERR] bonjour: Failed to bind to udp4 port: %v", err)
	}
	ipv6conn, err := net.ListenUDP("udp6", mdnsWildcardAddrIPv6)
	if err != nil {
		log.Printf("[ERR] bonjour: Failed to bind to udp6 port: %v", err)
	}
	if ipv4conn == nil && ipv6conn == nil {
		return nil, fmt.Errorf("[ERR] bonjour: Failed to bind to any udp port")
	}

	// Join multicast groups to receive announcements
	var p1 *ipv4.PacketConn
	var p2 *ipv6.PacketConn
	// RFC 6762 11: send with IP TTL 255. Multicast loopback stays on: other mDNS
	// software on this host only hears us through it.
	if ipv4conn != nil {
		p1 = ipv4.NewPacketConn(ipv4conn)
		if err := errors.Join(p1.SetTTL(255), p1.SetMulticastTTL(255)); err != nil {
			log.Printf("[ERR] bonjour: Failed to set udp4 socket options: %v", err)
		}
	}
	if ipv6conn != nil {
		p2 = ipv6.NewPacketConn(ipv6conn)
		if err := errors.Join(p2.SetHopLimit(255), p2.SetMulticastHopLimit(255)); err != nil {
			log.Printf("[ERR] bonjour: Failed to set udp6 socket options: %v", err)
		}
	}
	if iface != nil {
		if p1 != nil {
			if err := p1.JoinGroup(iface, &net.UDPAddr{IP: mdnsGroupIPv4}); err != nil {
				return nil, err
			}
		}
		if p2 != nil {
			if err := p2.JoinGroup(iface, &net.UDPAddr{IP: mdnsGroupIPv6}); err != nil {
				return nil, err
			}
		}
	} else {
		ifaces, err := net.Interfaces()
		if err != nil {
			return nil, err
		}
		errCount1, errCount2 := 0, 0
		for _, iface := range ifaces {
			if p1 != nil {
				if err := p1.JoinGroup(&iface, &net.UDPAddr{IP: mdnsGroupIPv4}); err != nil {
					errCount1++
				}
			}
			if p2 != nil {
				if err := p2.JoinGroup(&iface, &net.UDPAddr{IP: mdnsGroupIPv6}); err != nil {
					errCount2++
				}
			}
		}
		if len(ifaces) == errCount1 && len(ifaces) == errCount2 {
			return nil, fmt.Errorf("failed to join multicast group on all interfaces")
		}
	}

	s := &Server{
		ipv4conn:     ipv4conn,
		ipv6conn:     ipv6conn,
		ttl:          60 * 5,
		shutdownChan: make(chan bool),
		initialEntry: initialEntry,
	}
	s.send = s.writePacket

	return s, nil
}

// Shutdown closes all udp connections and unregisters the service
func (s *Server) Shutdown() {
	log.Println("Shutting down")
	s.shutdown()
}

// TTL sets the TTL for DNS replies
func (s *Server) TTL(ttl uint32) {
	s.ttl = ttl
}

// Start listeners and waits for the shutdown signal from exit channel
func (s *Server) mainloop(entries chan *ServiceEntry) {
	entry := s.initialEntry

	defer s.waitGroup.Done()

	requests := make(chan *Request, 5)

	if s.ipv4conn != nil {
		s.waitGroup.Add(1)
		go s.recv(s.ipv4conn, requests)
	}
	if s.ipv6conn != nil {
		s.waitGroup.Add(1)
		go s.recv(s.ipv6conn, requests)
	}

	// From RFC6762
	//    The Multicast DNS responder MUST send at least two unsolicited
	//    responses, one second apart. To provide increased robustness against
	//    packet loss, a responder MAY send up to eight unsolicited responses,
	//    provided that the interval between unsolicited responses increases by
	//    at least a factor of two with every response sent.
	timeout := 1 * time.Second
	ticker := time.NewTicker(timeout)

	i := 0

	for {
		select {
		case <-s.shutdownChan:
			ticker.Stop()
			return

		case entry = <-entries:
			s.pending, s.pendingTimer = nil, nil
			s.lastMulticast = nil
			i = 0
			timeout = 1 * time.Second
			break

		case <-ticker.C:
			i++
			if i > 3 {
				timeout = 1 * time.Second
				i = 0
			}

			resp := new(dns.Msg)
			resp.MsgHdr.Response = true
			resp.MsgHdr.Authoritative = true
			resp.Answer = []dns.RR{}
			resp.Extra = []dns.RR{}

			s.composeLookupAnswers(entry, resp, s.ttl)

			if err := s.multicastRateLimited(resp); err != nil {
				log.Println("[ERR] bonjour: failed to send announcement:", err.Error())
			}
			ticker.Stop()
			ticker = time.NewTicker(timeout)
			timeout *= 2
			break


		case <-s.pendingTimer:
			s.sendDue(time.Now())

		case req := <-requests:
			s.handleQuery(entry, req)
		}
	}
}

// Shutdown server will close currently open connections & channel
func (s *Server) shutdown() error {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	s.unregister(s.initialEntry)

	if s.shouldShutdown {
		return nil
	}
	s.shouldShutdown = true

	s.shutdownChan <- true
	//s.shutdownChanProbe <- true

	if s.ipv4conn != nil {
		s.ipv4conn.Close()
	}
	if s.ipv6conn != nil {
		s.ipv6conn.Close()
	}

	log.Println("Waiting for goroutines to finish")
	s.waitGroup.Wait()

	return nil
}

type Request struct {
	from  net.Addr
	query dns.Msg
}

// recv is a long running routine to receive packets from an interface
func (s *Server) recv(c *net.UDPConn, requests chan *Request) {
	defer s.waitGroup.Done()

	if c == nil {
		return
	}

	f, _ := c.File()
	fd, _ := syscall.Dup(int(f.Fd()))

	// duplicating the fd because c.fd which holds the actual one gets garbage collected
	e := syscall.SetNonblock(fd, true)
	if e != nil {
		println("Could not set nonblock", e)
	}
	c.SetReadBuffer(4000000)
	if e != nil {
		println("Could not set buffer size", e)
	}
	buf := make([]byte, 65536)
	// Interface names by index. Dropped every minute so a renamed or replaced interface is noticed.
	zones := map[uint32]string{}
	zonesRead := time.Now()

	i := 0
	bytes := 0

	for !s.shouldShutdown {
		n, sa, err := syscall.Recvfrom(fd, buf, 0)
		var from *net.UDPAddr
		switch sa := sa.(type) {
		case *syscall.SockaddrInet4:
			from = &net.UDPAddr{IP: sa.Addr[0:], Port: sa.Port}
		case *syscall.SockaddrInet6:
			from = &net.UDPAddr{IP: sa.Addr[0:], Port: sa.Port}
			// Unicast replies to link-local sources need the zone to pick the interface
			if sa.ZoneId != 0 {
				if time.Since(zonesRead) > time.Minute {
					clear(zones)
					zonesRead = time.Now()
				}
				name, ok := zones[sa.ZoneId]
				if !ok {
					// a failed lookup is remembered too, as the empty name, so it is not repeated for every packet
					if ifi, err := net.InterfaceByIndex(int(sa.ZoneId)); err == nil {
						name = ifi.Name
					}
					zones[sa.ZoneId] = name
				}
				from.Zone = name
			}
		}

		if err != nil {
			n = 0
		}

		if n == 0 {
			time.Sleep(50 * time.Millisecond)
			i = 0
			bytes = 0

			continue
		}

		i++
		bytes += n

		request, err := s.parsePacket(buf[:n], from)
		if err != nil {
			//log.Printf("[ERR] bonjour: Failed to handle query: %v", err)
		} else {
			requests <- request
		}
	}
}

// parsePacket is used to parse an incoming packet
func (s *Server) parsePacket(packet []byte, from net.Addr) (*Request, error) {
	var request Request
	request.from = from
	// ErrTruncated only reports the TC bit; the message is fully unpacked
	if err := request.query.Unpack(packet); err != nil && err != dns.ErrTruncated {
		log.Printf("[ERR] bonjour: Failed to unpack packet: %v", err)
		return nil, err
	}
	return &request, nil
}

// handleQuery is used to handle an incoming query
func (s *Server) handleQuery(service *ServiceEntry, request *Request) error {
	// RFC 6762 18.3, 18.11: messages with a non-zero opcode or response code are ignored
	if request.query.Response || request.query.Opcode != dns.OpcodeQuery || request.query.Rcode != dns.RcodeSuccess {
		return nil
	}
	// RFC 6762 8.2: a probe carries its proposed records in the authority section
	probe := len(request.query.Ns) > 0
	// RFC 6762 7.2: known answers that follow a truncated query apply to the replies still
	// waiting for it, but not to a reply that another querier is waiting for too
	for _, p := range s.pending {
		if p.from == request.from.String() {
			p.msg.Answer = suppressKnown(p.msg.Answer, request.query.Answer)
		}
	}

	// RFC 6762 6.7: a query not sent from port 5353 comes from a plain DNS resolver
	legacy := request.from.(*net.UDPAddr).Port != 5353

	// Handle each question
	var (
		resp dns.Msg
		err  error
	)
	if len(request.query.Question) > 0 {
		for _, q := range request.query.Question {
			resp = dns.Msg{}
			resp.Response = true
			resp.Authoritative = true
			resp.Answer = []dns.RR{}
			resp.Extra = []dns.RR{}
			if err = s.handleQuestion(q, service, &resp); err != nil {
				log.Printf("[ERR] bonjour: failed to handle question %v: %v",
					q, err)
				continue
			}
			resp.Answer = suppressKnown(resp.Answer, request.query.Answer)
			if legacy {
				resp.Id = request.query.Id
				resp.Question = []dns.Question{q}
				for _, rrs := range [][]dns.RR{resp.Answer, resp.Extra} {
					for _, rr := range rrs {
						rr.Header().Class &^= cacheFlushBit
						rr.Header().Ttl = min(rr.Header().Ttl, 10)
					}
				}
			}
			if len(resp.Answer) > 0 {
				var to *net.UDPAddr // nil for a multicast reply
				if legacy || isUnicastQuestion(q) {
					to = request.from.(*net.UDPAddr)
				}
				// RFC 6762 6: a reply waits a random delay when other responders may be answering
				// too, which is the case for shared records and for any query with several
				// questions (6.3), or when the known answers of a truncated query are still to come (7.2).
				// A legacy reply answers one question of one resolver and is sent at once.
				delay := request.query.Truncated || len(request.query.Question) > 1 || slices.ContainsFunc(resp.Answer, isShared)
				// Answers that defend our unique records against a probe are sent at once (6.3).
				// A shared record needs no defending, so a probe cannot hurry or repeat one.
				defending := probe && !slices.ContainsFunc(resp.Answer, isShared)
				if delay && !legacy && !defending {
					d := resp
					s.deferReply(&d, to, request)
				} else if to != nil {
					// Send unicast
					if e := s.unicastResponse(&resp, to); e != nil {
						err = e
					}
				} else {
					// Send mulicast. A record may be repeated after 250 ms rather than a
					// second when it defends against a probe (RFC 6762 6).
					gap := time.Second
					if defending {
						gap = 250 * time.Millisecond
					}
					if e := s.multicastSpaced(&resp, gap); e != nil {
						err = e
					}
				}
			}
		}
	}

	return err
}

// pendingReply is a reply waiting out its delay
type pendingReply struct {
	msg *dns.Msg
	to  *net.UDPAddr // nil for a multicast reply
	// The one querier the reply is for, whose later known answers still apply to it.
	// Empty once a second querier is waiting for it.
	from string
	due  time.Time
}

// deferReply queues resp to be sent to addr after the delay the query calls for, joining
// it to a reply already waiting for the same destination (RFC 6762 6.4)
func (s *Server) deferReply(resp *dns.Msg, to *net.UDPAddr, request *Request) {
	delay := 20 + rand.Intn(101)
	if request.query.Truncated {
		delay = 400 + rand.Intn(101)
	}
	due := time.Now().Add(time.Duration(delay) * time.Millisecond)
	from := request.from.String()

	i := slices.IndexFunc(s.pending, func(p *pendingReply) bool {
		return (p.to == nil) == (to == nil) && (to == nil || p.to.String() == to.String())
	})
	if i < 0 {
		s.pending = append(s.pending, &pendingReply{msg: resp, to: to, from: from, due: due})
	} else {
		p := s.pending[i]
		p.msg.Answer = appendNew(p.msg.Answer, resp.Answer)
		p.msg.Extra = appendNew(p.msg.Extra, resp.Extra)
		if p.from != from {
			p.from = ""
		}
		// a truncated query holds the reply back until its known answers have had time to arrive
		if request.query.Truncated && due.After(p.due) {
			p.due = due
		}
	}
	s.armPending()
}

// armPending sets the timer for the earliest waiting reply
func (s *Server) armPending() {
	s.pendingTimer = nil
	if len(s.pending) == 0 {
		return
	}
	next := slices.MinFunc(s.pending, func(a, b *pendingReply) int { return a.due.Compare(b.due) })
	s.pendingTimer = time.After(time.Until(next.due))
}

// sendDue sends the waiting replies whose delay is over at now
func (s *Server) sendDue(now time.Time) {
	var waiting []*pendingReply
	for _, p := range s.pending {
		var err error
		switch {
		case p.due.After(now):
			waiting = append(waiting, p)
		case len(p.msg.Answer) == 0:
			// the querier turned out to know every answer
		case p.to != nil:
			err = s.unicastResponse(p.msg, p.to)
		default:
			err = s.multicastRateLimited(p.msg)
		}
		if err != nil {
			log.Println("[ERR] bonjour: failed to send reply:", err.Error())
		}
	}
	s.pending = waiting
	s.armPending()
}

// handleQuestion is used to handle an incoming question
func (s *Server) handleQuestion(q dns.Question, service *ServiceEntry, resp *dns.Msg) error {
	if service == nil {
		return nil
	}

	switch canonicalName(q.Name) {
	case canonicalName(service.HostName):
		s.composeHostAnswers(service, resp, s.ttl)
	case canonicalName(service.ServiceName()):
		s.composeBrowsingAnswers(service, resp, s.ttl)
	case canonicalName(service.ServiceInstanceName()):
		s.composeLookupAnswers(service, resp, s.ttl)
	case canonicalName(service.ServiceTypeName()):
		s.serviceTypeName(service, resp, s.ttl)
	}

	// Only records of the name asked about are answers: the service PTRs that
	// composeLookupAnswers adds for announcements do not answer an instance lookup
	name := canonicalName(q.Name)
	resp.Answer = slices.DeleteFunc(resp.Answer, func(rr dns.RR) bool {
		return canonicalName(rr.Header().Name) != name
	})

	return nil
}

func (s *Server) composeHostAnswers(service *ServiceEntry, resp *dns.Msg, ttl uint32) {

	for _, ipAddr := range service.AddrsIPv4 {
		a := &dns.A{
			Hdr: dns.RR_Header{
				Name:   service.HostName,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET | cacheFlushBit,
				Ttl:    ttl,
			},
			A: ipAddr,
		}
		resp.Answer = append(resp.Answer, a)
	}
	for _, ipAddr := range service.AddrsIPv6 {
		aaaa := &dns.AAAA{
			Hdr: dns.RR_Header{
				Name:   service.HostName,
				Rrtype: dns.TypeAAAA,
				Class:  dns.ClassINET | cacheFlushBit,
				Ttl:    ttl,
			},
			AAAA: ipAddr,
		}
		resp.Answer = append(resp.Answer, aaaa)
	}
}

func (s *Server) composeBrowsingAnswers(service *ServiceEntry, resp *dns.Msg, ttl uint32) {

	ptr := &dns.PTR{
		Hdr: dns.RR_Header{
			Name:   service.ServiceName(),
			Rrtype: dns.TypePTR,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Ptr: service.ServiceInstanceName(),
	}
	resp.Answer = append(resp.Answer, ptr)

	txt := &dns.TXT{
		Hdr: dns.RR_Header{
			Name:   service.ServiceInstanceName(),
			Rrtype: dns.TypeTXT,
			Class:  dns.ClassINET | cacheFlushBit,
			Ttl:    ttl,
		},
		Txt: service.Text,
	}
	srv := &dns.SRV{
		Hdr: dns.RR_Header{
			Name:   service.ServiceInstanceName(),
			Rrtype: dns.TypeSRV,
			Class:  dns.ClassINET | cacheFlushBit,
			Ttl:    ttl,
		},
		Priority: 0,
		Weight:   0,
		Port:     uint16(service.Port),
		Target:   service.HostName,
	}
	resp.Extra = append(resp.Extra, srv, txt)

	for _, ipAddr := range service.AddrsIPv4 {
		a := &dns.A{
			Hdr: dns.RR_Header{
				Name:   service.HostName,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET | cacheFlushBit,
				Ttl:    ttl,
			},
			A: ipAddr,
		}
		resp.Extra = append(resp.Extra, a)
	}
	for _, ipAddr := range service.AddrsIPv6 {
		aaaa := &dns.AAAA{
			Hdr: dns.RR_Header{
				Name:   service.HostName,
				Rrtype: dns.TypeAAAA,
				Class:  dns.ClassINET | cacheFlushBit,
				Ttl:    ttl,
			},
			AAAA: ipAddr,
		}
		resp.Extra = append(resp.Extra, aaaa)
	}
}

func (s *Server) composeLookupAnswers(service *ServiceEntry, resp *dns.Msg, ttl uint32) {
	// RFC 6762 10.2: the cache-flush bit goes on unique records only, never on the shared PTRs

	ptr := &dns.PTR{
		Hdr: dns.RR_Header{
			Name:   service.ServiceName(),
			Rrtype: dns.TypePTR,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Ptr: service.ServiceInstanceName(),
	}
	srv := &dns.SRV{
		Hdr: dns.RR_Header{
			Name:   service.ServiceInstanceName(),
			Rrtype: dns.TypeSRV,
			Class:  dns.ClassINET | cacheFlushBit,
			Ttl:    ttl,
		},
		Priority: 0,
		Weight:   0,
		Port:     uint16(service.Port),
		Target:   service.HostName,
	}
	txt := &dns.TXT{
		Hdr: dns.RR_Header{
			Name:   service.ServiceInstanceName(),
			Rrtype: dns.TypeTXT,
			Class:  dns.ClassINET | cacheFlushBit,
			Ttl:    ttl,
		},
		Txt: service.Text,
	}
	dnssd := &dns.PTR{
		Hdr: dns.RR_Header{
			Name:   service.ServiceTypeName(),
			Rrtype: dns.TypePTR,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Ptr: service.ServiceName(),
	}
	resp.Answer = append(resp.Answer, ptr, txt, srv, dnssd)

	for _, ipAddr := range service.AddrsIPv4 {
		a := &dns.A{
			Hdr: dns.RR_Header{
				Name:   service.HostName,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET | cacheFlushBit,
				Ttl:    ttl,
			},
			A: ipAddr,
		}
		resp.Extra = append(resp.Extra, a)
	}
	for _, ipAddr := range service.AddrsIPv6 {
		aaaa := &dns.AAAA{
			Hdr: dns.RR_Header{
				Name:   service.HostName,
				Rrtype: dns.TypeAAAA,
				Class:  dns.ClassINET | cacheFlushBit,
				Ttl:    ttl,
			},
			AAAA: ipAddr,
		}
		resp.Extra = append(resp.Extra, aaaa)
	}
}

func (s *Server) serviceTypeName(service *ServiceEntry, resp *dns.Msg, ttl uint32) {
	// From RFC6762
	// 9.  Service Type Enumeration
	//
	//    For this purpose, a special meta-query is defined.  A DNS query for
	//    PTR records with the name "_services._dns-sd._udp.<Domain>" yields a
	//    set of PTR records, where the rdata of each PTR record is the two-
	//    label <Service> name, plus the same domain, e.g.,
	//    "_http._tcp.<Domain>".
	dnssd := &dns.PTR{
		Hdr: dns.RR_Header{
			Name:   service.ServiceTypeName(),
			Rrtype: dns.TypePTR,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Ptr: service.ServiceName(),
	}
	resp.Answer = append(resp.Answer, dnssd)
}

// announceText sends a Text announcement with cache flush enabled
func (s *Server) announceText(service *ServiceEntry) {
	resp := new(dns.Msg)
	resp.MsgHdr.Response = true
	resp.MsgHdr.Authoritative = true

	txt := &dns.TXT{
		Hdr: dns.RR_Header{
			Name:   service.ServiceInstanceName(),
			Rrtype: dns.TypeTXT,
			Class:  dns.ClassINET | 1<<15,
			Ttl:    s.ttl,
		},
		Txt: service.Text,
	}

	resp.Answer = []dns.RR{txt}
	s.multicastResponse(resp)
}

func (s *Server) unregister(service *ServiceEntry) error {
	resp := new(dns.Msg)
	resp.MsgHdr.Response = true
	resp.MsgHdr.Authoritative = true
	resp.Answer = []dns.RR{}
	resp.Extra = []dns.RR{}
	s.composeLookupAnswers(service, resp, 0)
	return s.multicastResponse(resp)
}

// unicastResponse is used to send a unicast response packet
func (s *Server) unicastResponse(resp *dns.Msg, from net.Addr) error {
	buf, err := resp.Pack()
	if err != nil {
		return err
	}
	return s.send(buf, from.(*net.UDPAddr))
}

// multicastResponse us used to send a multicast response packet
func (s *Server) multicastResponse(msg *dns.Msg) error {
	buf, err := msg.Pack()
	if err != nil {
		log.Println("Failed to pack message!")
		return err
	}
	return s.send(buf, nil)
}

// multicastRateLimited multicasts the records of msg that were not multicast in the last second (RFC 6762 6)
func (s *Server) multicastRateLimited(msg *dns.Msg) error {
	return s.multicastSpaced(msg, time.Second)
}

// multicastSpaced multicasts the records of msg that were not multicast within the last gap.
// The limit is per record wherever it travels, so additional records count as well.
func (s *Server) multicastSpaced(msg *dns.Msg, gap time.Duration) error {
	now := time.Now()
	recent := func(rr dns.RR) bool {
		return now.Sub(s.lastMulticast[rr.String()]) < gap
	}
	msg.Answer = slices.DeleteFunc(msg.Answer, recent)
	if len(msg.Answer) == 0 {
		return nil
	}
	msg.Extra = slices.DeleteFunc(msg.Extra, recent)
	if s.lastMulticast == nil {
		s.lastMulticast = map[string]time.Time{}
	}
	for _, rrs := range [][]dns.RR{msg.Answer, msg.Extra} {
		for _, rr := range rrs {
			s.lastMulticast[rr.String()] = now
		}
	}
	return s.multicastResponse(msg)
}

func (s *Server) writePacket(buf []byte, addr *net.UDPAddr) error {
	if addr == nil {
		if s.ipv4conn != nil {
			s.ipv4conn.WriteTo(buf, ipv4Addr)
		}
		if s.ipv6conn != nil {
			s.ipv6conn.WriteTo(buf, ipv6Addr)
		}
		return nil
	}
	if addr.IP.To4() != nil {
		_, err := s.ipv4conn.WriteToUDP(buf, addr)
		return err
	}
	_, err := s.ipv6conn.WriteToUDP(buf, addr)
	return err
}

// suppressKnown removes the answers that known holds with at least half our TTL (RFC 6762 7.1).
// A unique record stays when another record of its set does: the cache-flush bit on the
// one we send would make its receivers drop the ones we left out (RFC 6762 10.2).
func suppressKnown(answers, known []dns.RR) []dns.RR {
	isKnown := func(rr dns.RR) bool {
		return slices.ContainsFunc(known, func(ka dns.RR) bool {
			return ka.Header().Ttl >= rr.Header().Ttl/2 && sameRecord(rr, ka)
		})
	}
	unknown := slices.DeleteFunc(slices.Clone(answers), isKnown)
	return slices.DeleteFunc(answers, func(rr dns.RR) bool {
		return isKnown(rr) && (isShared(rr) || !slices.ContainsFunc(unknown, func(u dns.RR) bool {
			return u.Header().Rrtype == rr.Header().Rrtype && canonicalName(u.Header().Name) == canonicalName(rr.Header().Name)
		}))
	})
}

// isShared reports whether other responders may hold records of rr's name and type.
// Of the records we publish, those are the PTRs.
func isShared(rr dns.RR) bool {
	return rr.Header().Rrtype == dns.TypePTR
}

// appendNew appends the records of src that dst does not already hold
func appendNew(dst, src []dns.RR) []dns.RR {
	for _, rr := range src {
		if !slices.ContainsFunc(dst, func(d dns.RR) bool { return sameRecord(d, rr) }) {
			dst = append(dst, rr)
		}
	}
	return dst
}

// sameRecord reports whether a and b have the same name, type, class and rdata
func sameRecord(a, b dns.RR) bool {
	ha, hb := a.Header(), b.Header()
	if ha.Rrtype != hb.Rrtype || ha.Class&^cacheFlushBit != hb.Class&^cacheFlushBit ||
		canonicalName(ha.Name) != canonicalName(hb.Name) {
		return false
	}
	switch ra := a.(type) {
	case *dns.PTR:
		rb, ok := b.(*dns.PTR)
		return ok && canonicalName(ra.Ptr) == canonicalName(rb.Ptr)
	case *dns.SRV:
		rb, ok := b.(*dns.SRV)
		return ok && ra.Priority == rb.Priority && ra.Weight == rb.Weight && ra.Port == rb.Port &&
			canonicalName(ra.Target) == canonicalName(rb.Target)
	case *dns.TXT:
		rb, ok := b.(*dns.TXT)
		return ok && slices.Equal(ra.Txt, rb.Txt)
	case *dns.A:
		rb, ok := b.(*dns.A)
		return ok && ra.A.Equal(rb.A)
	case *dns.AAAA:
		rb, ok := b.(*dns.AAAA)
		return ok && ra.AAAA.Equal(rb.AAAA)
	}
	return false
}

func isUnicastQuestion(q dns.Question) bool {
	// From RFC6762
	// 18.12.  Repurposing of Top Bit of qclass in Question Section
	//
	//    In the Question Section of a Multicast DNS query, the top bit of the
	//    qclass field is used to indicate that unicast responses are preferred
	//    for this particular question.  (See Section 5.4.)
	return q.Qclass&(1<<15) != 0
}

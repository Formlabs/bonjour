package bonjour

import (
	"errors"
	"fmt"
	"log"
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

// How many times the records are announced when the server starts or they change
const announceCount = 4

// Server structure incapsulates both IPv4/IPv6 UDP connections
type Server struct {
	// The wait before the first announcement and between the first two. Later waits double.
	announceInterval time.Duration
	// How often the records are announced again once the announcements above are
	// done. Zero for never. See mainloop for why this exists.
	keepAliveInterval time.Duration
	// The sockets as packet connections, for joining groups and choosing the interface
	// a multicast packet leaves on, and the interface the server is limited to, if any
	p4    *ipv4.PacketConn
	p6    *ipv6.PacketConn
	iface *net.Interface
	// Interface names on which the groups were joined. Owned by mainloop once it runs.
	joined4, joined6 map[string]bool
	ipv4conn         *net.UDPConn
	ipv6conn         *net.UDPConn
	initialEntry     *ServiceEntry
	shouldShutdown   bool
	shutdownChan     chan bool
	waitGroup        sync.WaitGroup
	shutdownLock     sync.Mutex
	ttl              uint32
	// send writes a packet to addr, or to both multicast groups when addr is nil
	send func(buf []byte, addr *net.UDPAddr) error
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
	s := &Server{
		ipv4conn:          ipv4conn,
		ipv6conn:          ipv6conn,
		p4:                p1,
		p6:                p2,
		iface:             iface,
		joined4:           map[string]bool{},
		joined6:           map[string]bool{},
		ttl:               60 * 5,
		announceInterval:  time.Second,
		keepAliveInterval: 150 * time.Second,
		shutdownChan:      make(chan bool),
		initialEntry:      initialEntry,
	}
	if !s.joinGroups() {
		return nil, fmt.Errorf("failed to join multicast group on all interfaces")
	}
	s.send = s.writePacket

	return s, nil
}

// multicastInterfaces returns the interfaces the server works on: the one it was
// given, or every interface the host has at this moment
func (s *Server) multicastInterfaces() []net.Interface {
	if s.iface != nil {
		return []net.Interface{*s.iface}
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		log.Printf("[ERR] bonjour: Failed to list interfaces: %v", err)
	}
	return ifaces
}

// joinGroups joins the mDNS groups on the server's interfaces and reports whether it
// is a member on any. It runs when the server starts and again whenever the records
// change: an interface that had no address at the start, or did not exist yet, such
// as Wi-Fi joined after boot, could not be joined then, and its queries would never
// reach us. Joining an interface a second time fails, which does no harm.
func (s *Server) joinGroups() bool {
	joined := false
	for _, ifi := range s.multicastInterfaces() {
		if s.p4 != nil {
			err := s.p4.JoinGroup(&ifi, &net.UDPAddr{IP: mdnsGroupIPv4})
			if err == nil && !s.joined4[ifi.Name] {
				log.Printf("bonjour: joined the IPv4 mDNS group on %s", ifi.Name)
			}
			if err == nil {
				s.joined4[ifi.Name] = true
			}
			joined = joined || s.joined4[ifi.Name]
		}
		if s.p6 != nil {
			err := s.p6.JoinGroup(&ifi, &net.UDPAddr{IP: mdnsGroupIPv6})
			if err == nil && !s.joined6[ifi.Name] {
				log.Printf("bonjour: joined the IPv6 mDNS group on %s", ifi.Name)
			}
			if err == nil {
				s.joined6[ifi.Name] = true
			}
			joined = joined || s.joined6[ifi.Name]
		}
	}
	return joined
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
	//
	// The records are announced announceCount times when the server starts and again
	// whenever they change, 1, 2, 4 and 8 intervals after that moment. From there on
	// they are sent in answer to queries, and once every keepAliveInterval.
	//
	// The keep-alive is a hedge. On networks that drop most multicast packets on the
	// way to clients (e.g. observed at Formlabs HQ on Wi-Fi), a client can miss both the
	// announcements and the answers to its queries, and before fixing FE-29496 the endless
	// announcing covered for that. If answers to queries prove enough on such networks,
	// the keep-alive can go.
	remaining := announceCount
	wait := s.announceInterval
	timer := time.NewTimer(wait)

	for {
		select {
		case <-s.shutdownChan:
			timer.Stop()
			return

		case entry = <-entries:
			remaining = announceCount
			wait = s.announceInterval
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(wait)
			// the addresses changed, so an interface may have come up
			s.joinGroups()

		case <-timer.C:
			resp := new(dns.Msg)
			resp.MsgHdr.Response = true
			resp.MsgHdr.Authoritative = true
			resp.Answer = []dns.RR{}
			resp.Extra = []dns.RR{}

			s.composeLookupAnswers(entry, resp, s.ttl)

			if err := s.multicastResponse(resp); err != nil {
				log.Println("[ERR] bonjour: failed to send announcement:", err.Error())
			}
			if remaining > 0 {
				remaining--
			}
			if remaining > 0 {
				timer.Reset(wait)
				wait *= 2
			} else if s.keepAliveInterval > 0 {
				timer.Reset(s.keepAliveInterval)
			}

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
	// ErrTruncated only reports the TC bit; the message is fully unpacked. A truncated
	// query is answered like any other. Its known answers go on in further packets,
	// which are not waited for: at worst we repeat an answer the querier listed there.
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
	// Ignore questions with Authorative section for now
	if len(request.query.Ns) > 0 {
		return nil
	}

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
			if len(resp.Answer) > 0 {
				if isUnicastQuestion(q) {
					// Send unicast
					if e := s.unicastResponse(&resp, request.from); e != nil {
						err = e
					}
				} else {
					// Send mulicast
					if e := s.multicastResponse(&resp); e != nil {
						err = e
					}
				}
			}
		}
	}

	return err
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

func (s *Server) writePacket(buf []byte, addr *net.UDPAddr) error {
	if addr == nil {
		// Multicast on every interface that is up. Left to the routing table, a
		// multicast packet leaves on one interface only, and a printer on both
		// Ethernet and Wi-Fi would announce itself and answer on just one of them.
		sent := false
		for _, ifi := range s.multicastInterfaces() {
			const want = net.FlagUp | net.FlagMulticast
			if ifi.Flags&want != want || ifi.Flags&net.FlagLoopback != 0 {
				continue
			}
			if s.p4 != nil {
				if _, err := s.p4.WriteTo(buf, &ipv4.ControlMessage{IfIndex: ifi.Index}, ipv4Addr); err == nil {
					sent = true
				}
			}
			if s.p6 != nil {
				if _, err := s.p6.WriteTo(buf, &ipv6.ControlMessage{IfIndex: ifi.Index}, ipv6Addr); err == nil {
					sent = true
				}
			}
		}
		if sent {
			return nil
		}
		// no interface took the packet: fall back on the routing table's choice
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

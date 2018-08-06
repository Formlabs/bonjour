package bonjour

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"encoding/binary"
	"errors"

	"github.com/miekg/dns"
	"golang.org/x/net/ipv4"
)

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

// Register a service by given arguments. This call will take the system's hostname
// and lookup IP by that hostname.
func Register(instance, service, domain string, port int, text []string, iface *net.Interface) (*Server, error) {
	entry := NewServiceEntry(instance, service, domain)
	entry.Port = port
	entry.Text = text

	if entry.Instance == "" {
		return nil, fmt.Errorf("Missing service instance name")
	}
	if entry.Service == "" {
		return nil, fmt.Errorf("Missing service name")
	}
	if entry.Domain == "" {
		entry.Domain = "local"
	}
	if entry.Port == 0 {
		return nil, fmt.Errorf("Missing port")
	}

	var err error
	if entry.HostName == "" {
		entry.HostName, err = os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("Could not determine host")
		}
	}
	entry.HostName = fmt.Sprintf("%s.", trimDot(entry.HostName))

	addrs, err := net.LookupIP(entry.HostName)
	if err != nil {
		// Try appending the host domain suffix and lookup again
		// (required for Linux-based hosts)
		tmpHostName := fmt.Sprintf("%s%s.", entry.HostName, entry.Domain)
		addrs, err = net.LookupIP(tmpHostName)
		if err != nil {
			return nil, fmt.Errorf("Could not determine host IP addresses for %s", entry.HostName)
		}
	}
	for i := 0; i < len(addrs); i++ {
		if ipv4 := addrs[i].To4(); ipv4 != nil {
			entry.AddrIPv4 = addrs[i]
		} else if ipv6 := addrs[i].To16(); ipv6 != nil {
			entry.AddrIPv6 = addrs[i]
		}
	}

	s, err := newServer(iface)
	if err != nil {
		return nil, err
	}

	s.service = entry
	//go s.mainloop()
	go s.probe()

	return s, nil
}

// Register a service proxy by given argument. This call will skip the hostname/IP lookup and
// will use the provided values.
func RegisterProxy(instance, service, domain string, port int, host, ip string, text []string, iface *net.Interface) (*Server, error) {
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

	ipAddr := net.ParseIP(ip)
	if ipAddr == nil {
		return nil, fmt.Errorf("Failed to parse given IP: %v", ip)
	} else if ipv4 := ipAddr.To4(); ipv4 != nil {
		entry.AddrIPv4 = ipAddr
	} else if ipv6 := ipAddr.To16(); ipv6 != nil {
		entry.AddrIPv4 = ipAddr
	} else {
		return nil, fmt.Errorf("The IP is neither IPv4 nor IPv6: %#v", ipAddr)
	}

	s, err := newServer(iface)
	if err != nil {
		return nil, err
	}

	s.service = entry
	go s.mainloop()
	go s.probe()

	return s, nil
}

// Server structure incapsulates both IPv4/IPv6 UDP connections
type Server struct {
	service        *ServiceEntry
	ipv4conn       *net.UDPConn
	ipv6conn       *net.UDPConn
	shouldShutdown bool
	shutdownLock   sync.Mutex
	ttl            uint32
}

// Constructs server structure
func newServer(iface *net.Interface) (*Server, error) {
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
		return nil, fmt.Errorf("[ERR] bonjour: Failed to bind to any udp port!")
	}

	// Join multicast groups to receive announcements
	p1 := ipv4.NewPacketConn(ipv4conn)
	//p2 := ipv6.NewPacketConn(ipv6conn)
	if iface != nil {
		if err := p1.JoinGroup(iface, &net.UDPAddr{IP: mdnsGroupIPv4}); err != nil {
			return nil, err
		}
		//if err := p2.JoinGroup(iface, &net.UDPAddr{IP: mdnsGroupIPv6}); err != nil {
		//	return nil, err
		//}
	} else {
		ifaces, err := net.Interfaces()
		if err != nil {
			return nil, err
		}
		errCount1, errCount2 := 0, 0
		for _, iface := range ifaces {
			if err := p1.JoinGroup(&iface, &net.UDPAddr{IP: mdnsGroupIPv4}); err != nil {
				errCount1++
			}
			//if err := p2.JoinGroup(&iface, &net.UDPAddr{IP: mdnsGroupIPv6}); err != nil {
			//	errCount2++
			//}
		}
		if len(ifaces) == errCount1 && len(ifaces) == errCount2 {
			return nil, fmt.Errorf("Failed to join multicast group on all interfaces!")
		}
	}

	s := &Server{
		ipv4conn: ipv4conn,
		//ipv6conn: ipv6conn,
		ttl:      3200,
	}

	return s, nil
}

// Start listeners and waits for the shutdown signal from exit channel
func (s *Server) mainloop() {
	if s.ipv4conn != nil {
		go s.recv(s.ipv4conn)
	}
	if s.ipv6conn != nil {
		go s.recv(s.ipv6conn)
	}
}

// Shutdown closes all udp connections and unregisters the service
func (s *Server) Shutdown() {
	s.shutdown()
}

// SetText updates and announces the TXT records
func (s *Server) SetText(text []string) {
	s.service.Text = text
	s.announceText()
}

// TTL sets the TTL for DNS replies
func (s *Server) TTL(ttl uint32) {
	s.ttl = ttl
}

// Shutdown server will close currently open connections & channel
func (s *Server) shutdown() error {
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	s.unregister()

	if s.shouldShutdown {
		return nil
	}
	s.shouldShutdown = true

	if s.ipv4conn != nil {
		s.ipv4conn.Close()
	}
	if s.ipv6conn != nil {
		s.ipv6conn.Close()
	}
	return nil
}

// recv is a long running routine to receive packets from an interface
func (s *Server) recv(c *net.UDPConn) {
	if c == nil {
		return
	}
	buf := make([]byte, 65536)
	for !s.shouldShutdown {
		n, from, err := c.ReadFrom(buf)
		if err != nil {
			continue
		}
		if err := s.parsePacket(buf[:n], from); err != nil {
			log.Printf("[ERR] bonjour: Failed to handle query: %v", err)
		}
	}
}

// parsePacket is used to parse an incoming packet
func (s *Server) parsePacket(packet []byte, from net.Addr) error {
	var msg dns.Msg
	if err := msg.Unpack(packet); err != nil {
	//if err := UnpackDnsMsg(&msg, packet); err != nil {
		log.Printf("[ERR] bonjour: Failed to unpack packet: %v", err)
		return err
	}
	return s.handleQuery(&msg, from)
}


func unpackUint16(msg []byte, off int) (i uint16, off1 int, err error) {
	if off+2 > len(msg) {
		return 0, len(msg), errors.New("overflow")
	}
	return binary.BigEndian.Uint16(msg[off:]), off + 2, nil
}

func unpackMsgHdr(msg []byte, off int) (dns.Header, int, error) {
	var (
		dh  dns.Header
		err error
	)
	dh.Id, off, err = unpackUint16(msg, off)
	if err != nil {
		return dh, off, err
	}
	dh.Bits, off, err = unpackUint16(msg, off)
	if err != nil {
		return dh, off, err
	}
	dh.Qdcount, off, err = unpackUint16(msg, off)
	if err != nil {
		return dh, off, err
	}
	dh.Ancount, off, err = unpackUint16(msg, off)
	if err != nil {
		return dh, off, err
	}
	dh.Nscount, off, err = unpackUint16(msg, off)
	if err != nil {
		return dh, off, err
	}
	dh.Arcount, off, err = unpackUint16(msg, off)
	return dh, off, err
}

const (
	headerSize = 12

	// Header.Bits
	_QR = 1 << 15 // query/response (response=1)
	_AA = 1 << 10 // authoritative
	_TC = 1 << 9  // truncated
	_RD = 1 << 8  // recursion desired
	_RA = 1 << 7  // recursion available
	_Z  = 1 << 6  // Z
	_AD = 1 << 5  // authticated data
	_CD = 1 << 4  // checking disabled
)

func unpackQuestion(msg []byte, off int) (dns.Question, int, error) {
	var (
		q   dns.Question
		err error
	)
	q.Name, off, err = dns.UnpackDomainName(msg, off)
	if err != nil {
		return q, off, err
	}
	if off == len(msg) {
		return q, off, nil
	}
	q.Qtype, off, err = unpackUint16(msg, off)
	if err != nil {
		return q, off, err
	}
	if off == len(msg) {
		return q, off, nil
	}
	q.Qclass, off, err = unpackUint16(msg, off)
	if off == len(msg) {
		return q, off, nil
	}
	return q, off, err
}

// unpackRRslice unpacks msg[off:] into an []RR.
// If we cannot unpack the whole array, then it will return nil
func unpackRRslice(l int, msg []byte, off int) (dst1 []dns.RR, off1 int, err error) {
	var r dns.RR
	// Don't pre-allocate, l may be under attacker control
	var dst []dns.RR
	for i := 0; i < l; i++ {
		off1 := off
		r, off, err = dns.UnpackRR(msg, off)
		//println("RR slice", r.String())
		if err != nil {
			off = len(msg)
			break
		}
		// If offset does not increase anymore, l is a lie
		if off1 == off {
			l = i
			break
		}
		dst = append(dst, r)
	}
	if err != nil && off == len(msg) {
		dst = nil
	}
	return dst, off, err
}

// Unpack unpacks a binary message to a Msg structure.
func UnpackDnsMsg(m *dns.Msg, msg []byte) (err error) {
	var (
		dh  dns.Header
		off int
	)
	if dh, off, err = unpackMsgHdr(msg, off); err != nil {
		return err
	}

	m.Id = dh.Id
	m.Response = (dh.Bits & _QR) != 0
	m.Opcode = int(dh.Bits>>11) & 0xF
	m.Authoritative = (dh.Bits & _AA) != 0
	m.Truncated = (dh.Bits & _TC) != 0
	m.RecursionDesired = (dh.Bits & _RD) != 0
	m.RecursionAvailable = (dh.Bits & _RA) != 0
	m.Zero = (dh.Bits & _Z) != 0
	m.AuthenticatedData = (dh.Bits & _AD) != 0
	m.CheckingDisabled = (dh.Bits & _CD) != 0
	m.Rcode = int(dh.Bits & 0xF)

	// If we are at the end of the message we should return *just* the
	// header. This can still be useful to the caller. 9.9.9.9 sends these
	// when responding with REFUSED for instance.
	if off == len(msg) {
		// reset sections before returning
		m.Question, m.Answer, m.Ns, m.Extra = nil, nil, nil, nil
		return nil
	}

	// Qdcount, Ancount, Nscount, Arcount can't be trusted, as they are
	// attacker controlled. This means we can't use them to pre-allocate
	// slices.
	m.Question = nil
	for i := 0; i < int(dh.Qdcount); i++ {
		off1 := off
		var q dns.Question
		q, off, err = unpackQuestion(msg, off)
		//println("Unpack question ", q.Name, q.Qclass, q.Qtype)
		if q.Qtype != dns.TypePTR {
			//println("Skipping type", q.Qtype, dns.TypePTR)
			return nil
		}
		if err != nil {
			// Even if Truncated is set, we only will set ErrTruncated if we
			// actually got the questions
			return err
		}
		if off1 == off { // Offset does not increase anymore, dh.Qdcount is a lie!
			dh.Qdcount = uint16(i)
			break
		}
		m.Question = append(m.Question, q)
	}

	m.Answer, off, err = unpackRRslice(int(dh.Ancount), msg, off)
	//// The header counts might have been wrong so we need to update it
	dh.Ancount = uint16(len(m.Answer))
	if err == nil {
		m.Ns, off, err = unpackRRslice(int(dh.Nscount), msg, off)
	}
	//println("Answer count", dh.Ancount)
	//// The header counts might have been wrong so we need to update it
	//dh.Nscount = uint16(len(m.Ns))
	//if err == nil {
	//	m.Extra, off, err = unpackRRslice(int(dh.Arcount), msg, off)
	//}
	dh.Nscount = 0
	// The header counts might have been wrong so we need to update it
	dh.Arcount = uint16(len(m.Extra))


	if off != len(msg) {
		// TODO(miek) make this an error?
		// use PackOpt to let people tell how detailed the error reporting should be?
		// println("m: extra bytes in m packet", off, "<", len(msg))
	} else if m.Truncated {
		// Whether we ran into a an error or not, we want to return that it
		// was truncated
		err = dns.ErrTruncated
	}
	return err
}

// handleQuery is used to handle an incoming query
func (s *Server) handleQuery(query *dns.Msg, from net.Addr) error {
	// Ignore answer for now
	if len(query.Answer) > 0 {
		return nil
	}
	// Ignore questions with Authorative section for now
	if len(query.Ns) > 0 {
		return nil
	}

	// Handle each question
	var (
		resp dns.Msg
		err  error
	)
	if len(query.Question) > 0 {
		for _, q := range query.Question {
			resp = dns.Msg{}
			resp.SetReply(query)
			resp.Answer = []dns.RR{}
			resp.Extra = []dns.RR{}
			if err = s.handleQuestion(q, &resp); err != nil {
				log.Printf("[ERR] bonjour: failed to handle question %v: %v",
					q, err)
				continue
			}
			// Check if there is an answer
			if len(resp.Answer) > 0 {
				if isUnicastQuestion(q) {
					// Send unicast
					if e := s.unicastResponse(&resp, from); e != nil {
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
func (s *Server) handleQuestion(q dns.Question, resp *dns.Msg) error {
	if s.service == nil {
		return nil
	}

	println("Match", q.Name)
	println("Match", q.Name, "hostname", s.service.HostName)
	//println("Match", q.Name, "to", s.service.ServiceName())
	//println("MatchInstance", q.Name, "to", s.service.ServiceInstanceName())
	//println("MatchService", q.Name, "to", s.service.ServiceTypeName())
	if (q.Name == s.service.HostName) {
		println("Match", q.Name, "MATCHED MATCHED")
	}

	switch q.Name {
	case s.service.HostName:
		println("Match", q.Name, "Matched to host", s.service.HostName)
		s.composeHostAnswers(resp, s.ttl)
	case s.service.ServiceName():
		s.composeBrowsingAnswers(resp, s.ttl)
	case s.service.ServiceInstanceName():
		s.composeLookupAnswers(resp, s.ttl)
	case s.service.ServiceTypeName():
		s.serviceTypeName(resp, s.ttl)
	}

	return nil
}

func (s *Server) composeHostAnswers(resp *dns.Msg, ttl uint32) {
	if s.service.AddrIPv4 != nil {
		a := &dns.A{
			Hdr: dns.RR_Header{
				Name:   s.service.HostName,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET | dns.TypeTA,
				Ttl:    ttl,
			},
			A: s.service.AddrIPv4,
		}
		resp.Answer = append(resp.Answer, a)
	}
	if s.service.AddrIPv6 != nil {
		aaaa := &dns.AAAA{
			Hdr: dns.RR_Header{
				Name:   s.service.HostName,
				Rrtype: dns.TypeAAAA,
				Class:  dns.ClassINET | dns.TypeTA,
				Ttl:    ttl,
			},
			AAAA: s.service.AddrIPv6,
		}
		resp.Answer = append(resp.Answer, aaaa)
	}

	resp.Question = make([]dns.Question, 0)
}

func (s *Server) composeBrowsingAnswers(resp *dns.Msg, ttl uint32) {
	ptr := &dns.PTR{
		Hdr: dns.RR_Header{
			Name:   s.service.ServiceName(),
			Rrtype: dns.TypePTR,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Ptr: s.service.ServiceInstanceName(),
	}
	resp.Answer = append(resp.Answer, ptr)

	txt := &dns.TXT{
		Hdr: dns.RR_Header{
			Name:   s.service.ServiceInstanceName(),
			Rrtype: dns.TypeTXT,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Txt: s.service.Text,
	}
	srv := &dns.SRV{
		Hdr: dns.RR_Header{
			Name:   s.service.ServiceInstanceName(),
			Rrtype: dns.TypeSRV,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Priority: 0,
		Weight:   0,
		Port:     uint16(s.service.Port),
		Target:   s.service.HostName,
	}
	resp.Extra = append(resp.Extra, srv, txt)

	if s.service.AddrIPv4 != nil {
		a := &dns.A{
			Hdr: dns.RR_Header{
				Name:   s.service.HostName,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET,
				Ttl:    ttl,
			},
			A: s.service.AddrIPv4,
		}
		resp.Extra = append(resp.Extra, a)
	}
	if s.service.AddrIPv6 != nil {
		aaaa := &dns.AAAA{
			Hdr: dns.RR_Header{
				Name:   s.service.HostName,
				Rrtype: dns.TypeAAAA,
				Class:  dns.ClassINET,
				Ttl:    ttl,
			},
			AAAA: s.service.AddrIPv6,
		}
		resp.Extra = append(resp.Extra, aaaa)
	}
}

func (s *Server) composeLookupAnswers(resp *dns.Msg, ttl uint32) {
	// From RFC6762
	//    The most significant bit of the rrclass for a record in the Answer
	//    Section of a response message is the Multicast DNS cache-flush bit
	//    and is discussed in more detail below in Section 10.2, "Announcements
	//    to Flush Outdated Cache Entries".
	cache_flush := uint16(1 << 15)
	ptr := &dns.PTR{
		Hdr: dns.RR_Header{
			Name:   s.service.ServiceName(),
			Rrtype: dns.TypePTR,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Ptr: s.service.ServiceInstanceName(),
	}
	srv := &dns.SRV{
		Hdr: dns.RR_Header{
			Name:   s.service.ServiceInstanceName(),
			Rrtype: dns.TypeSRV,
			Class:  dns.ClassINET | cache_flush,
			Ttl:    ttl,
		},
		Priority: 0,
		Weight:   0,
		Port:     uint16(s.service.Port),
		Target:   s.service.HostName,
	}
	txt := &dns.TXT{
		Hdr: dns.RR_Header{
			Name:   s.service.ServiceInstanceName(),
			Rrtype: dns.TypeTXT,
			Class:  dns.ClassINET | cache_flush,
			Ttl:    ttl,
		},
		Txt: s.service.Text,
	}
	dnssd := &dns.PTR{
		Hdr: dns.RR_Header{
			Name:   s.service.ServiceTypeName(),
			Rrtype: dns.TypePTR,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Ptr: s.service.ServiceName(),
	}
	resp.Answer = append(resp.Answer, srv, txt, ptr, dnssd)

	if s.service.AddrIPv4 != nil {
		a := &dns.A{
			Hdr: dns.RR_Header{
				Name:   s.service.HostName,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET | cache_flush,
				Ttl:    120,
			},
			A: s.service.AddrIPv4,
		}
		resp.Extra = append(resp.Extra, a)
	}
	if s.service.AddrIPv6 != nil {
		aaaa := &dns.AAAA{
			Hdr: dns.RR_Header{
				Name:   s.service.HostName,
				Rrtype: dns.TypeAAAA,
				Class:  dns.ClassINET | cache_flush,
				Ttl:    120,
			},
			AAAA: s.service.AddrIPv6,
		}
		resp.Extra = append(resp.Extra, aaaa)
	}
}

func (s *Server) serviceTypeName(resp *dns.Msg, ttl uint32) {
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
			Name:   s.service.ServiceTypeName(),
			Rrtype: dns.TypePTR,
			Class:  dns.ClassINET,
			Ttl:    ttl,
		},
		Ptr: s.service.ServiceName(),
	}
	resp.Answer = append(resp.Answer, dnssd)
}

// Perform probing & announcement
//TODO: implement a proper probing & conflict resolution
func (s *Server) probe() {
	q := new(dns.Msg)
	q.SetQuestion(s.service.ServiceInstanceName(), dns.TypePTR)
	q.RecursionDesired = false

	srv := &dns.SRV{
		Hdr: dns.RR_Header{
			Name:   s.service.ServiceInstanceName(),
			Rrtype: dns.TypeSRV,
			Class:  dns.ClassINET,
			Ttl:    s.ttl,
		},
		Priority: 0,
		Weight:   0,
		Port:     uint16(s.service.Port),
		Target:   s.service.HostName,
	}
	txt := &dns.TXT{
		Hdr: dns.RR_Header{
			Name:   s.service.ServiceInstanceName(),
			Rrtype: dns.TypeTXT,
			Class:  dns.ClassINET,
			Ttl:    s.ttl,
		},
		Txt: s.service.Text,
	}
	q.Ns = []dns.RR{srv, txt}

	randomizer := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 3; i++ {
		if err := s.multicastResponse(q); err != nil {
			log.Println("[ERR] bonjour: failed to send probe:", err.Error())
		}
		time.Sleep(time.Duration(randomizer.Intn(250)) * time.Millisecond)
	}
	resp := new(dns.Msg)
	resp.MsgHdr.Response = true
	resp.Answer = []dns.RR{}
	resp.Extra = []dns.RR{}
	s.composeLookupAnswers(resp, s.ttl)

	// From RFC6762
	//    The Multicast DNS responder MUST send at least two unsolicited
	//    responses, one second apart. To provide increased robustness against
	//    packet loss, a responder MAY send up to eight unsolicited responses,
	//    provided that the interval between unsolicited responses increases by
	//    at least a factor of two with every response sent.
	timeout := 1 * time.Second
	for !s.shouldShutdown {
		for i := 0; i < 3 && !s.shouldShutdown; i++ {
			if err := s.multicastResponse(resp); err != nil {
				log.Println("[ERR] bonjour: failed to send announcement:", err.Error())
			}
			time.Sleep(timeout)
			timeout *= 2
		}
	}
}

// announceText sends a Text announcement with cache flush enabled
func (s *Server) announceText() {
	resp := new(dns.Msg)
	resp.MsgHdr.Response = true

	txt := &dns.TXT{
		Hdr: dns.RR_Header{
			Name:   s.service.ServiceInstanceName(),
			Rrtype: dns.TypeTXT,
			Class:  dns.ClassINET | 1<<15,
			Ttl:    s.ttl,
		},
		Txt: s.service.Text,
	}

	resp.Answer = []dns.RR{txt}
	s.multicastResponse(resp)
}

func (s *Server) unregister() error {
	resp := new(dns.Msg)
	resp.MsgHdr.Response = true
	resp.Answer = []dns.RR{}
	resp.Extra = []dns.RR{}
	s.composeLookupAnswers(resp, 0)
	return s.multicastResponse(resp)
}

// unicastResponse is used to send a unicast response packet
func (s *Server) unicastResponse(resp *dns.Msg, from net.Addr) error {
	buf, err := resp.Pack()
	if err != nil {
		return err
	}
	addr := from.(*net.UDPAddr)
	if addr.IP.To4() != nil {
		_, err = s.ipv4conn.WriteToUDP(buf, addr)
		return err
	} else {
		_, err = s.ipv6conn.WriteToUDP(buf, addr)
		return err
	}
}

// multicastResponse us used to send a multicast response packet
func (c *Server) multicastResponse(msg *dns.Msg) error {
	buf, err := msg.Pack()
	if err != nil {
		log.Println("Failed to pack message!")
		return err
	}
	if c.ipv4conn != nil {
		c.ipv4conn.WriteTo(buf, ipv4Addr)
	}
	if c.ipv6conn != nil {
		c.ipv6conn.WriteTo(buf, ipv6Addr)
	}
	return nil
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

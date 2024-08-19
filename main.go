package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multiaddr"
	"github.com/xunterr/crawler/internal/frontier"

	golog "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
)

func main() {
	bootstrapNode := flag.String("b", "", "bootstrap node")
	seedUrl := flag.String("u", "", "seed url")
	port := flag.Int("p", 6969, "port number")
	flag.Parse()

	golog.SetAllLoggers(golog.LevelInfo)

	key, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		log.Fatalln(err)
		return
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", *port)),
		libp2p.Identity(key),
		libp2p.DefaultTransports,
		libp2p.DefaultMuxers,
		libp2p.DefaultSecurity,
		libp2p.NATPortMap(),
	}

	basicHost, err := libp2p.New(opts...)
	if err != nil {
		log.Fatalln(err)
		return
	}

	ctx := context.Background()

	dht, err := dht.New(ctx, basicHost, dht.Mode(dht.ModeAutoServer))
	if err != nil {
		log.Fatalln(err)
		return
	}

	if *bootstrapNode != "" {
		bootstrapFrom(context.Background(), basicHost, []string{*bootstrapNode})
	}

	if err = dht.Bootstrap(ctx); err != nil {
		log.Fatalln(err)
		return
	}

	hostAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ipfs/%s", basicHost.ID()))
	log.Println("I can be reached at: ")
	for _, addr := range basicHost.Addrs() {
		log.Println(addr.Encapsulate(hostAddr))
	}

	time.Sleep(500 * time.Millisecond)
	frontier, err := frontier.NewBfFrontier()
	if err != nil {
		log.Fatalln(err)
		return
	}

	dispatcher := NewDispatcher(basicHost, dht, frontier.Put)

	parsedUrl, err := url.Parse(*seedUrl)
	if err != nil {
		log.Fatalln("Failed to parse url")
		return
	}

	err = dispatcher.Dispatch(*parsedUrl)
	if err != nil {
		log.Fatalln(err)
		return
	}

	for {
		url, accessAt, err := frontier.Get()
		if err != nil {
			log.Println(err)
			return
		}

		go func() {

			time.Sleep(time.Until(accessAt))

			time.Sleep(250 * time.Millisecond)
			frontier.Processed(*url.URL, time.Duration(250*time.Millisecond))

			frontier.Put(*url.URL)

			log.Println(url.String())
		}()
	}
}

func bootstrapFrom(ctx context.Context, host host.Host, bootstrapNodes []string) {
	for _, bootstrapNode := range bootstrapNodes {
		bootstrapMaddr := multiaddr.StringCast(bootstrapNode)
		bootstrapPeerInfo, err := peer.AddrInfoFromP2pAddr(bootstrapMaddr)
		if err != nil {
			log.Printf("Failed to bootstrap from node %s", bootstrapNode)
			continue
		}

		go func() {
			host.Peerstore().AddAddrs(bootstrapPeerInfo.ID, bootstrapPeerInfo.Addrs, peerstore.PermanentAddrTTL)
			if err := host.Connect(context.Background(), *bootstrapPeerInfo); err != nil {
				log.Printf("Failed to connect to node %s", bootstrapPeerInfo.String())
				return
			} else {
				log.Printf("Bootstraped with node %s", bootstrapPeerInfo.ID)
			}
		}()
	}
}

func SendMessage(s network.Stream, message []byte) ([]byte, error) {
	_, err := s.Write(message)
	if err != nil {
		return nil, err
	}
	buf := bufio.NewReader(s)
	str, err := buf.ReadString('\n')
	if err != nil {
		return nil, err
	}

	return []byte(str), err
}

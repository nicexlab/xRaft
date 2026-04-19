package conn

import (
	"context"
	"github/Fischer0522/xraft/xraft/pb"
	"github/Fischer0522/xraft/xraft/request"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewLocalCoordinator(peers []*Participants_2PC, id int) *Coordinator {
	servers := make([]xraftServer, len(peers))
	for i := range peers {
		servers[i] = peers[i]
	}
	return newCoordinator(servers, id)
}

type coordinatorGRPC struct {
	conn   *grpc.ClientConn
	server pb.XRaftServerClient
}

func newCoordinatorGRPC(peer string) *coordinatorGRPC {
	c := &coordinatorGRPC{}

	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("net.Connect err: %v", err)
	}
	c.conn = conn
	c.server = pb.NewXRaftServerClient(conn)
	return c
}

func (c *coordinatorGRPC) AbortReq(cmdID request.RequestID, fTerm uint32) (reply *pb.RequestReply) {
	r, err := c.server.AbortReqGrpc(context.Background(), &pb.AbortReq{FTerm: fTerm, ReqID: &pb.RequestID{SeqId: cmdID.SeqID, ClientID: cmdID.ClientID}})
	if err != nil {
		log.Fatal(err)
	}
	return r
}

func (c *coordinatorGRPC) Propose(mess *pb.Message) *pb.MessageReply {
	r, err := c.server.ProposeGrpc(context.Background(), mess)
	if err != nil {
		log.Fatal(err)
	}
	return r
}
func (c *coordinatorGRPC) StartFast(cmd *pb.FBFCmd) *pb.MessageReply {
	r, err := c.server.StartFast(context.Background(), cmd)
	if err != nil {
		log.Fatal(err)
	}
	return r
}

func (c *coordinatorGRPC) Ping() int64 {
	mess := &pb.Message{}
	start := time.Now()
	c.server.Ping(context.Background(), mess)
	return time.Since(start).Microseconds()
}

func (c *coordinatorGRPC) PingAndWrite(size int) int64 {
	mess := &pb.Message{}
	mess.Request = &pb.Request{Command: &pb.Command{Key: "test", Val: strings.Repeat("a", size)}}
	start := time.Now()
	c.server.Ping(context.Background(), mess)
	return time.Since(start).Milliseconds()
}

func NewGRPCCoordinator(peers []string, id int) (*Coordinator, []func() int64, []func(int) int64) {
	servers := make([]xraftServer, len(peers))
	ps := make([]*coordinatorGRPC, len(peers))
	fs := make([]func() int64, len(peers))
	fs2 := make([]func(int) int64, len(peers))
	for i := range peers {
		ps[i] = newCoordinatorGRPC(peers[i])
		servers[i] = ps[i]
		fs[i] = ps[i].Ping
		fs2[i] = ps[i].PingAndWrite
	}
	return newCoordinator(servers, id), fs, fs2
}

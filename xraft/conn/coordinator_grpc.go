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

func Init_Coordinator_2PC(peers []*Participants_2PC, id int) *Coordinator {
	servers := make([]xraftServer, len(peers))
	for i := range peers {
		servers[i] = peers[i]
	}
	return Init_Coordinator(servers, id)
}

type coor_grpc struct {
	conn   *grpc.ClientConn
	server pb.XRaftServerClient
}

func new_coor_grpc(peer string) *coor_grpc {
	c := &coor_grpc{}

	conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("net.Connect err: %v", err)
	}
	c.conn = conn
	c.server = pb.NewXRaftServerClient(conn)
	return c
}

func (c *coor_grpc) AbortReq(cmdID request.RequestID, FTerm uint32) (reply *pb.RequestReply) {
	r, err := c.server.AbortReqGrpc(context.Background(), &pb.AbortReq{FTerm: FTerm, ReqID: &pb.RequestID{SeqId: cmdID.SeqID, ClientID: cmdID.ClientID}})
	if err != nil {
		log.Fatal(err)
	}
	return r
}

func (c *coor_grpc) Propose(mess *pb.Message) *pb.MessageReply {
	r, err := c.server.ProposeGrpc(context.Background(), mess)
	if err != nil {
		log.Fatal(err)
	}
	return r
}
func (c *coor_grpc) StartFast(cmd *pb.FBFCmd) *pb.MessageReply {
	r, err := c.server.StartFast(context.Background(), cmd)
	if err != nil {
		log.Fatal(err)
	}
	return r
}

func (c *coor_grpc) Ping() int64 {
	mess := &pb.Message{}
	start := time.Now()
	c.server.Ping(context.Background(), mess)
	return time.Since(start).Microseconds()
}

func (c *coor_grpc) PingAndWrite(size int) int64 {
	mess := &pb.Message{}
	mess.Request = &pb.Request{Command: &pb.Command{Key: "test", Val: strings.Repeat("a", size)}}
	start := time.Now()
	c.server.Ping(context.Background(), mess)
	return time.Since(start).Milliseconds()
}

func Init_Coordinator_Grpc(peers []string, id int) (*Coordinator, []func() int64, []func(int) int64) {
	servers := make([]xraftServer, len(peers))
	ps := make([]*coor_grpc, len(peers))
	fs := make([]func() int64, len(peers))
	fs2 := make([]func(int) int64, len(peers))
	for i := range peers {
		ps[i] = new_coor_grpc(peers[i])
		servers[i] = ps[i]
		fs[i] = ps[i].Ping
		fs2[i] = ps[i].PingAndWrite
	}
	return Init_Coordinator(servers, id), fs, fs2
}

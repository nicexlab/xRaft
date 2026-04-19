package conn

import (
	"context"
	"github/Fischer0522/xraft/xraft/pb"
	"github/Fischer0522/xraft/xraft/request"
	"log"
	"net"

	"google.golang.org/grpc"
)

type grpcServer struct {
	// *xproto.UnimplementedXRaftServerServer
	*pb.UnimplementedXRaftServerServer
	part *Participants_2PC
}

func StartGRPCServer(p *Participants_2PC, address string) {
	gserver := &grpcServer{
		part: p,
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("net.Listen err: %v", err)
	}
	log.Println(address + " listening...")
	grpcServer := grpc.NewServer()
	pb.RegisterXRaftServerServer(grpcServer, gserver)
	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatalf("grpcServer.Serve err: %v", err)
	}
}

func (s *grpcServer) ProposeGrpc(ctx context.Context, in *pb.Message) (*pb.MessageReply, error) {
	return s.part.Propose(in), nil
}

func (s *grpcServer) AbortReqGrpc(ctx context.Context, in *pb.AbortReq) (*pb.RequestReply, error) {
	reqID := request.RequestID{ClientID: in.ReqID.ClientID, SeqID: in.ReqID.SeqId}
	return s.part.AbortReq(reqID, in.FTerm), nil
}

func (s *grpcServer) Ping(ctx context.Context, in *pb.Message) (*pb.MessageReply, error) {
	if in.Request != nil {
		s.part.kvStore.PutWithOldValue(in.Request.Command.Key, in.Request.Command.Val)
	}
	return &pb.MessageReply{}, nil
}

func (s *grpcServer) StartFast(ctx context.Context, in *pb.FBFCmd) (*pb.MessageReply, error) {
	s.part.ExecuteFBFCmds(in)
	return &pb.MessageReply{}, nil
}

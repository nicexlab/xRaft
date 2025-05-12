package curp

import (
	"context"
	"github/Fischer0522/xraft/curp/command"
	"github/Fischer0522/xraft/curp/curp_proto"
	pb "github/Fischer0522/xraft/curp/curp_proto"
	"log"
	"net"

	"google.golang.org/grpc"
)

type grpc_server struct {
	*pb.UnimplementedCurpServer
	state *StateMachine
}

func Startgrpc(state *StateMachine, Address string) {
	gserver := &grpc_server{
		state: state,
	}

	listener, err := net.Listen("tcp", Address)
	if err != nil {
		log.Fatalf("net.Listen err: %v", err)
	}
	log.Println(Address + " net.Listing...")
	grpcServer := grpc.NewServer()
	curp_proto.RegisterCurpServer(grpcServer, gserver)
	// xproto.RegisterXRaftServerServer(grpcServer, gserver)
	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatalf("grpcServer.Serve err: %v", err)
	}
}

func (s *grpc_server) IsLeader(context.Context, *pb.LeaderAck) (*pb.LeaderReply, error) {
	ret := &pb.LeaderReply{}
	if s.state.IsLeader() {
		ret.IsLeader = 1
	} else {
		ret.IsLeader = 0
	}
	return ret, nil
}
func (s *grpc_server) Propose(context context.Context, in *pb.CurpClientCommand) (*pb.CurpReply, error) {
	return s.state.Propose(in), nil
}

func (s *grpc_server) WaitSynced(ctx context.Context, in *pb.ProposeId) (*pb.CurpReply, error) {
	return s.state.WaitSynced(command.ProposeId{ClientId: in.ClientId, SeqId: in.SeqId}), nil
}

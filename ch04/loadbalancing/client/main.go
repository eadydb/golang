package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	ecpb "google.golang.org/grpc/examples/features/proto/echo"
	"google.golang.org/grpc/resolver"
	"log"
	"time"
)

const (
	exampleScheme      = "example"
	exampleServiceName = "lb.example.grpc.io"
)

var addrs = []string{"localhost:50051", "localhost:50052"}

func callUnaryEcho(c ecpb.EchoClient, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.UnaryEcho(ctx, &ecpb.EchoRequest{
		Message: message,
	})
	if err != nil {
		log.Fatalf("cloud not greet: %v", err)
	}
	fmt.Println(r.Message)
}

func makeRPCs(cc *grpc.ClientConn, n int) {
	hwc := ecpb.NewEchoClient(cc)
	for i := 0; i < n; i++ {
		callUnaryEcho(hwc, "this is examples/load_balancing")
	}
}

func main() {
	pickFirstConn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", exampleScheme, exampleServiceName),
		grpc.WithInsecure(),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer pickFirstConn.Close()

	log.Println("==== calling helloworld.Greeter/SayHello with pick_first =====")
	makeRPCs(pickFirstConn, 10)

	roundrobinConn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", exampleScheme, exampleServiceName),
		grpc.WithBalancerName("round_robin"),
		grpc.WithInsecure(),
	)

	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	log.Println("=== calling helloworld.Greeter/SayHello with round_robin ===")
	makeRPCs(roundrobinConn, 10)
}

type exampleResolverBuilder struct {
}

func (*exampleResolverBuilder) Build(target resolver.Target, conn resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &exampleResolver{
		target: target,
		cc:     conn,
		addressStore: map[string][]string{
			exampleServiceName: addrs,
		},
	}
	r.start()
	return r, nil
}

func (*exampleResolverBuilder) Scheme() string {
	return exampleScheme
}

type exampleResolver struct {
	target       resolver.Target
	cc           resolver.ClientConn
	addressStore map[string][]string
}

func (r *exampleResolver) start() {
	addsStrs := r.addressStore[r.target.Endpoint]
	addrs := make([]resolver.Address, len(addsStrs))
	for i, s := range addsStrs {
		addrs[i] = resolver.Address{Addr: s}
	}
	r.cc.UpdateState(resolver.State{Addresses: addrs})
}

func (*exampleResolver) ResolveNow(o resolver.ResolveNowOptions) {}
func (*exampleResolver) Close()                                  {}

func init() {
	resolver.Register(&exampleResolverBuilder{})
}

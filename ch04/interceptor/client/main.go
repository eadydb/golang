package main

import (
	"context"
	pb "github.com/eadydb/grpc-samples/ch03/proto"
	"github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/grpc"
	"io"
	"log"
	"time"
)

const (
	address = "localhost:50051"
)

func main() {

	conn, err := grpc.Dial(address, grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(orderUnaryClientInterceptor),
		grpc.WithStreamInterceptor(clientStreamInterceptor))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewOrderManagementClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// ===========================================
	// Search Order : Server streaming
	//retrievedOrder, err := c.GetOrder(ctx, &wrappers.StringValue{Value: "106"})
	//if err != nil {
	//	log.Print(err)
	//}
	//log.Print("GetOrder Response -> : ", retrievedOrder)
	//
	//searchStream, _ := c.SearchOrders(ctx, &wrappers.StringValue{Value: "Google"})
	//for {
	//	searchOrder, err := searchStream.Recv()
	//	if err == io.EOF {
	//		break
	//	}
	//	log.Print("Search Result : ", searchOrder)
	//}

	// ===========================================
	// Update Orders : Client stream
	//updOrder1 := pb.Order{Id: "102", Items:[]string{"Google Pixel 3A", "Google Pixel Book"}, Destination:"Mountain View, CA", Price:1100.00}
	//updOrder2 := pb.Order{Id: "103", Items:[]string{"Apple Watch S4", "Mac Book Pro", "iPad Pro"}, Destination:"San Jose, CA", Price:2800.00}
	//updOrder3 := pb.Order{Id: "104", Items:[]string{"Google Home Mini", "Google Nest Hub", "iPad Mini"}, Destination:"Mountain View, CA", Price:2200.00}
	//
	//updateStream, err := c.UpdateOrders(ctx)
	//
	//if err != nil {
	//	log.Fatalf("%v.UpdateOrders(_) = _, %v", c, err)
	//}
	//
	//if err := updateStream.Send(&updOrder1); err != nil {
	//	log.Fatalf("%v.Send(%v) = %v", updateStream, updOrder1.String(), err)
	//}
	//
	//if err := updateStream.Send(&updOrder2); err != nil {
	//	log.Fatalf("%v.Send(%v) = %v", updateStream, updOrder2.String(), err)
	//}
	//
	//if err := updateStream.Send(&updOrder3); err != nil {
	//	log.Fatalf("%v.Send(%v) = %v", updateStream, updOrder3.String(), err)
	//}
	//
	//updateRes, err := updateStream.CloseAndRecv()
	//if err != nil {
	//	log.Fatalf("%v.CloseAndRecv() got error %v, want %v", updateStream, err, nil)
	//}
	//log.Printf("Update Orders Res : %s", updateRes)

	// ===========================================
	// Process Order : Bi-di streaming scenario
	streamProcOrder, err := c.ProcessOrders(ctx)
	if err != nil {
		log.Fatalf("%v.ProcessOrders(_) = _, %v", c, err)
	}

	if err := streamProcOrder.Send(&wrappers.StringValue{Value: "102"}); err != nil {
		log.Fatalf("%v.Send(%v) = %v", c, "102", err)
	}

	if err := streamProcOrder.Send(&wrappers.StringValue{Value: "103"}); err != nil {
		log.Fatalf("%v.Send(%v) = %v", c, "103", err)
	}

	if err := streamProcOrder.Send(&wrappers.StringValue{Value: "104"}); err != nil {
		log.Fatalf("%v.Send(%v) = %v", c, "104", err)
	}

	channel := make(chan struct{})
	go asyncClientBidirectionalRPC(streamProcOrder, channel)
	time.Sleep(time.Millisecond * 1000)

	if err := streamProcOrder.Send(&wrappers.StringValue{Value: "101"}); err != nil {
		log.Fatalf("%v.Send(%v) = %v", c, "101", err)
	}
	if err := streamProcOrder.CloseSend(); err != nil {
		log.Fatal(err)
	}
	<-channel
}

func asyncClientBidirectionalRPC(streamProcOrder pb.OrderManagement_ProcessOrdersClient, c chan struct{}) {
	for {
		combinedShipment, errProcOrder := streamProcOrder.Recv()
		if errProcOrder == io.EOF {
			break
		}
		if combinedShipment != nil {
			log.Printf("Combined shipment : %v", combinedShipment.OrderList)
		}
	}
	<-c
}

func orderUnaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// Pre-processor phase
	log.Println("Method : " + method)

	// Invoking the remote method
	err := invoker(ctx, method, req, reply, cc, opts...)

	// Post-processor phase
	log.Println(reply)

	return err
}

func clientStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer,
	opts ...grpc.CallOption) (grpc.ClientStream, error) {

	log.Println("======= [Client Interceptor] ", method)
	s, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}
	return newWrappedStream(s), nil
}

type wrappedStream struct {
	grpc.ClientStream
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	log.Printf("====== [Client Stream Interceptor] Receive a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	log.Printf("====== [Client Stream Interceptor] Send a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.SendMsg(m)
}

func newWrappedStream(s grpc.ClientStream) grpc.ClientStream {
	return &wrappedStream{s}
}

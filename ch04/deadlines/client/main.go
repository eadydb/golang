package main

import (
	"context"
	pb "github.com/eadydb/grpc-samples/ch04/deadlines/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"log"
	"time"
)

const (
	address = "localhost:50051"
)

func main() {

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewOrderManagementClient(conn)

	// Deadlines and timeouts are two commonly used patterns in distributed computing.
	// imeouts allow you to specify how long a client application can wait for an RPC to complete before it terminates with an error.
	// A timeout is usually specified as a duration and locally applied at each client side. For example,
	// a single request may consist of multiple downstream RPCs that chain together multiple services.
	// So we can apply timeouts, relative to each RPC, at each service invocation. Therefore,
	// timeouts cannot be directly applied for the entire life cycle of the request. That’s where we need to use deadlines.
	clientDeadline := time.Now().Add(time.Duration(2 * time.Second))

	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	// Add Order
	order1 := pb.Order{Id: "101", Items: []string{"iPhone XS", "Mac Book Pro"}, Destination: "San Jose, CA", Price: 2300.00}
	res, addErr := c.AddOrder(ctx, &order1)

	if addErr != nil {
		got := status.Code(addErr)
		log.Printf("Error Occured -> addOrder : , %v:", got)
	} else {
		log.Print("AddOrder Response -> ", res.Value)
	}

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
	//streamProcOrder, err := c.ProcessOrders(ctx)
	//if err != nil {
	//	log.Fatalf("%v.ProcessOrders(_) = _, %v", c, err)
	//}
	//
	//if err := streamProcOrder.Send(&wrappers.StringValue{Value:"102"}); err != nil {
	//	log.Fatalf("%v.Send(%v) = %v", c, "102", err)
	//}
	//
	//if err := streamProcOrder.Send(&wrappers.StringValue{Value:"103"}); err != nil {
	//	log.Fatalf("%v.Send(%v) = %v", c, "103", err)
	//}
	//
	//if err := streamProcOrder.Send(&wrappers.StringValue{Value:"104"}); err != nil {
	//	log.Fatalf("%v.Send(%v) = %v", c, "104", err)
	//}
	//
	//channel := make(chan struct{})
	//go asyncClientBidirectionalRPC(streamProcOrder, channel)
	//time.Sleep(time.Millisecond * 1000)
	//
	//if err := streamProcOrder.Send(&wrappers.StringValue{Value:"101"}); err != nil {
	//	log.Fatalf("%v.Send(%v) = %v", c, "101", err)
	//}
	//if err := streamProcOrder.CloseSend(); err != nil {
	//	log.Fatal(err)
	//}
	//<- channel
}

//func asyncClientBidirectionalRPC(streamProcOrder pb.OrderManagement_ProcessOrdersClient, c chan struct{}) {
//	for {
//		combinedShipment, errProcOrder := streamProcOrder.Recv()
//		if errProcOrder == io.EOF {
//			break
//		}
//		if combinedShipment != nil {
//			log.Printf("Combined shipment : %v", combinedShipment.OrderList)
//		}
//	}
//	<-c
//}

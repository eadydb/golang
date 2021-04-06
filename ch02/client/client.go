package main

import (
	"context"
	pb "github.com/eadydb/grpc-samples/ch02/proto"
	"google.golang.org/grpc"
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
	c := pb.NewProductInfoClient(conn)

	// Contact the server and print out its response
	name := "Apple iphone 11"
	description := "Meet Apple iphone 11. All-new dual-camera system with Ultra Wide and Night mode."
	price := float32(699.0)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.AddProduct(ctx, &pb.Product{
		Name:        name,
		Description: description,
		Price:       price,
	})
	if err != nil {
		log.Fatalf("Could not add product: %v", err)
	}
	log.Printf("Product ID : %s added successfully", r.Value)

	product, err := c.GetProduct(ctx, &pb.ProductID{Value: r.Value})
	if err != nil {
		log.Fatalf("could not get product: %v", err)
	}

	log.Printf("Product: %v", product.String())

}

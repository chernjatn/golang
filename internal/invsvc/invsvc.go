package invsvc

import (
	context "context"
	"errors"
	"fmt"
	"main/internal/cache"
	"main/internal/dbinv"
	"main/internal/redisinv"
	"net"
	"strconv"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	status "google.golang.org/grpc/status"
)

type server struct {
	InvsvcServer
	rclient   redisinv.RedisInvContract
	cache     cache.Cache
	discounts discountsvc
}

func (s *server) fromRProductInventoryFull(
	ctx context.Context,
	in *GetRegionInventoryRequest,
	invItem *redisinv.RProductInventoryFull,
) (*ProductInventoryFull, error) {
	prices, err := s.discounts.calc(ctx, uint(in.RegionId), uint(in.ProductId), uint(in.Quantity), invItem.Price)
	if err != nil {
		return nil, status.Error(400, err.Error())
	}

	return &ProductInventoryFull{
		Quantity:      uint32(invItem.Quantity),
		Price:         prices.Price,
		DiscountPrice: prices.DiscountPrice,
		Sum:           prices.Sum,
		DiscountSum:   prices.DiscountSum,
		ValidDate:     invItem.ValidDate,
		IsId:          uint32(invItem.InventorySourceId),
		StoreId:       uint32(invItem.StoreId),
		IsType:        InventorySourceType(invItem.InventorySourceType),
		IsVaries:      invItem.IsVaries,
	}, nil
}

func (s *server) GetInventory(ctx context.Context, in *GetInventoryRequest) (*GetInventoryResponse, error) {
	if in.IsId == 0 {
		return nil, status.Error(400, "no is id")
	}
	if in.ProductId == 0 {
		return nil, status.Error(400, "no product id")
	}

	cacheKey := fmt.Sprintf("inv:%d:%d:%d", in.RegionId, in.IsId, in.ProductId)

	cb := func() (interface{}, error) {
		inv, err := s.rclient.GetInventory(uint(in.ProductId), uint(in.IsId))
		if err != nil {
			return nil, status.Error(404, err.Error())
		}

		prices, err := s.discounts.calc(ctx, uint(in.RegionId), uint(in.ProductId), uint(in.Quantity), inv.Price)
		if err != nil {
			return nil, status.Error(400, err.Error())
		}

		return &GetInventoryResponse{
			Item: &ProductInventory{
				Quantity:      uint32(inv.Quantity),
				Price:         prices.Price,
				DiscountPrice: prices.DiscountPrice,
				Sum:           prices.Sum,
				DiscountSum:   prices.DiscountSum,
				ValidDate:     inv.ValidDate,
			},
		}, nil
	}

	var (
		val interface{}
		err error
	)

	if in.Quantity > 1 {
		val, err = cb()
	} else {
		val, err = s.cache.Remeber(cacheKey, 60*time.Second, cb)
	}

	if err != nil {
		return nil, err
	}

	return val.(*GetInventoryResponse), nil
}

func (s *server) GetRegionInventory(ctx context.Context, in *GetRegionInventoryRequest) (*GetRegionInventoryResponse, error) {
	if in.RegionId == 0 {
		return nil, status.Error(400, "no region id")
	}
	if in.ProductId == 0 {
		return nil, status.Error(400, "no product id")
	}

	cacheKey := "ri:" + strconv.FormatUint(uint64(in.RegionId), 10) + ":" + strconv.FormatUint(uint64(in.ProductId), 10)

	cb := func() (interface{}, error) {
		invItems, err := s.rclient.GetRegionInventory(uint(in.RegionId), uint(in.ProductId))
		if err != nil {
			return nil, status.Error(404, err.Error())
		}

		var result []*ProductInventoryFull

		for _, invItem := range invItems {
			newInvItem, err := s.fromRProductInventoryFull(ctx, in, invItem)
			if err != nil {
				return nil, status.Error(400, err.Error())
			}

			result = append(result, newInvItem)
		}

		return &GetRegionInventoryResponse{Items: result}, nil
	}

	var (
		val interface{}
		err error
	)

	if in.Quantity > 1 {
		val, err = cb()
	} else {
		val, err = s.cache.Remeber(cacheKey, 60*time.Second, cb)
	}

	if err != nil {
		return nil, err
	}

	return val.(*GetRegionInventoryResponse), nil
}

func (s *server) GetRegionMaxInventory(ctx context.Context, in *GetRegionInventoryRequest) (*GetRegionMaxInventoryResponse, error) {
	if in.RegionId == 0 {
		return nil, status.Error(400, "no region id")
	}
	if in.ProductId == 0 {
		return nil, status.Error(400, "no product id")
	}

	cacheKey := "rmi:" + strconv.FormatUint(uint64(in.RegionId), 10) + ":" + strconv.FormatUint(uint64(in.ProductId), 10)

	cb := func() (interface{}, error) {
		invItem, err := s.rclient.GetRegionMaxInventory(uint(in.RegionId), uint(in.ProductId))
		if err != nil {
			return nil, status.Error(404, err.Error())
		}

		newInvItem, err := s.fromRProductInventoryFull(ctx, in, invItem)
		if err != nil {
			return nil, status.Error(400, err.Error())
		}

		return &GetRegionMaxInventoryResponse{Item: newInvItem}, nil
	}

	var (
		val interface{}
		err error
	)

	if in.Quantity > 1 {
		val, err = cb()
	} else {
		val, err = s.cache.Remeber(cacheKey, 60*time.Second, cb)
	}

	if err != nil {
		return nil, err
	}

	return val.(*GetRegionMaxInventoryResponse), nil
}

func (s *server) GetRegionLastInventory(ctx context.Context, in *GetRegionInventoryRequest) (*GetRegionLastInventoryResponse, error) {
	if in.RegionId == 0 {
		return nil, status.Error(400, "no region id")
	}
	if in.ProductId == 0 {
		return nil, status.Error(400, "no product id")
	}

	cacheKey := "rli:" + strconv.FormatUint(uint64(in.RegionId), 10) + ":" + strconv.FormatUint(uint64(in.ProductId), 10)

	cb := func() (interface{}, error) {
		invItem, err := s.rclient.GetRegionLastInventory(uint(in.RegionId), uint(in.ProductId))
		if err != nil {
			return nil, status.Error(404, err.Error())
		}

		newInvItem, err := s.fromRProductInventoryFull(ctx, in, invItem)
		if err != nil {
			return nil, status.Error(400, err.Error())
		}

		return &GetRegionLastInventoryResponse{Item: newInvItem}, nil
	}

	var (
		val interface{}
		err error
	)

	if in.Quantity > 1 {
		val, err = cb()
	} else {
		val, err = s.cache.Remeber(cacheKey, 60*time.Second, cb)
	}

	if err != nil {
		return nil, err
	}

	return val.(*GetRegionLastInventoryResponse), nil
}

func (s *server) GetRegionStoreHas(ctx context.Context, in *GetRegionInventoryRequest) (*GetRegionStoreHasResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *server) GetRegionStoreHasCnt(ctx context.Context, in *GetRegionInventoryRequest) (*GetRegionStoreHasCntResponse, error) {
	if in.RegionId == 0 {
		return nil, status.Error(400, "no region id")
	}
	if in.ProductId == 0 {
		return nil, status.Error(400, "no product id")
	}

	cacheKey := "rshc:" + strconv.FormatUint(uint64(in.RegionId), 10) + ":" + strconv.FormatUint(uint64(in.ProductId), 10)

	val, err := s.cache.Remeber(
		cacheKey,
		60*time.Second,
		func() (interface{}, error) {
			result, err := s.rclient.GetRegionStCntInventory(uint(in.RegionId), uint(in.ProductId))
			if err != nil {
				return nil, status.Error(404, err.Error())
			}

			return &GetRegionStoreHasCntResponse{Cnt: uint32(result)}, nil
		},
	)
	if err != nil {
		return nil, err
	}

	return val.(*GetRegionStoreHasCntResponse), nil
}

func StartServer(ir dbinv.InventoryRepositoryContract, rclient redisinv.RedisInvContract, addr string) error {
	var server = &server{
		rclient:   rclient,
		cache:     cache.GetCache(),
		discounts: discountsvc{ir: ir},
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	s := grpc.NewServer()
	RegisterInvsvcServer(s, server)

	reflection.Register(s)

	fmt.Println("invsvc ready")

	cacheTicker := time.NewTicker(time.Hour * 2)
	defer cacheTicker.Stop()

	discountsTicker := time.NewTicker(time.Minute * 5)
	defer discountsTicker.Stop()

	go func() {
		server.discounts.syncDiscounts()

		for {
			select {
			case <-cacheTicker.C:
				fmt.Println("clear start")
				server.cache.Clear()
				fmt.Println("clear end")
			case <-discountsTicker.C:
				fmt.Println("discounts start")
				server.discounts.syncDiscounts()
				fmt.Println("discounts end")
			}
		}
	}()

	return s.Serve(lis)
}

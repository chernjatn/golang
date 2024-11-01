package redisinv

import (
	"context"
	"fmt"
	"main/internal/dbinv"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

var oneTimeLimiter = make(chan bool, 10)

type RProductInventory struct {
	Quantity  uint
	Price     uint64
	ValidDate string
}

type RProductInventoryFull struct {
	RProductInventory
	InventorySourceType uint
	InventorySourceId   uint
	StoreId             uint
	IsVaries            bool
}

type RedisInvContract interface {
	// GetRProductInventory(isId uint, productId int) (*RProductInventory, error)
	UpdateInventorySource(isId uint, items map[uint]RProductInventory) error
	TruncateRegionInventory(regionId uint)
	UpdateRegionInventory(regionId uint, items map[uint][]RProductInventoryFull) error
	UpdateRegionStInventory(regionId uint, items map[uint][]uint) error
	UpdateRegionMaxInventory(regionId uint, items map[uint]RProductInventoryFull) error
	UpdateRegionLastInventory(regionId uint, items map[uint]RProductInventoryFull) error
	GetRegionInventoryPrId(regionId uint) ([]uint, error)
	GetRegionStInventoryPrId(regionId uint) ([]uint, error)
	GetRegionMaxInventoryPrId(regionId uint) ([]uint, error)
	DeleteRegionInventory(regionId uint, productIds []uint) error
	DeleteRegionStInventory(regionId uint, productIds []uint) error
	DeleteRegionMaxInventory(regionId uint, productIds []uint) error

	GetInventory(productId uint, isId uint) (*RProductInventory, error)
	GetRegionInventory(regionId uint, productId uint) ([]*RProductInventoryFull, error)
	GetRegionStCntInventory(regionId uint, productId uint) (uint, error)
	GetRegionMaxInventory(regionId uint, productId uint) (*RProductInventoryFull, error)
	GetRegionLastInventory(regionId uint, productId uint) (*RProductInventoryFull, error)
}

type RedisInvRepository struct {
	client *redis.Client
	ctx    *context.Context
}

func ISPFromProductInventory(item dbinv.ProductInventory) RProductInventory {
	return RProductInventory{
		Quantity:  item.Qty.Int,
		Price:     item.Price.Int,
		ValidDate: item.ValidDate,
	}
}

func ISPFullStCollSerialize(items []uint) string {
	var result = ""

	for _, item := range items {
		result += strconv.FormatUint(uint64(item), 10) + "\n"
	}

	return result
}

func ISPFullStCollUnSerialize(serialized string) (uint, error) {
	var result uint = 0

	for indx := range serialized {
		if serialized[indx] == '\n' {
			result++
		}
	}

	return result, nil
}

func ISPFullCollSerialize(items []RProductInventoryFull) string {
	var result = ""

	for _, item := range items {
		result += ISPFullSerialize(&item) + "\n"
	}

	return result
}

func ISPFullCollUnSerialize(serialized string) ([]*RProductInventoryFull, error) {
	var items []*RProductInventoryFull

	var fAcc strings.Builder

	for indx := range serialized {
		if serialized[indx] == '\n' {
			if fAcc.Len() != 0 {
				unserialized, err := ISPFullUnserialize(fAcc.String())
				if err != nil {
					return nil, err
				}
				items = append(items, unserialized)
			}

			fAcc.Reset()
			continue
		}
		fAcc.WriteByte(serialized[indx])
	}

	return items, nil
}

func ISPFullSerialize(pr *RProductInventoryFull) string {
	var partResult = ISPSerialize(&RProductInventory{
		Quantity:  pr.Quantity,
		Price:     pr.Price,
		ValidDate: pr.ValidDate,
	})

	return partResult +
		strconv.FormatUint(uint64(pr.StoreId), 10) +
		"|" +
		strconv.FormatUint(uint64(pr.InventorySourceId), 10) +
		"|" +
		strconv.FormatUint(uint64(pr.InventorySourceType), 10) +
		"|" +
		strconv.FormatBool(pr.IsVaries) +
		"|"
}

func ISPFullUnserialize(serialized string) (*RProductInventoryFull, error) {
	partResult, err := ISPUnserialize(serialized)
	if err != nil {
		return nil, err
	}

	result := &RProductInventoryFull{}

	result.Quantity = partResult.Quantity
	result.Price = partResult.Price
	result.ValidDate = partResult.ValidDate

	var fAcc strings.Builder
	var fIndex = 0
L:
	for indx := range serialized {
		if serialized[indx] == '|' {
			switch fIndex {
			case 3:
				fVal, err := strconv.ParseUint(fAcc.String(), 10, 32)
				if err != nil {
					return nil, err
				}
				result.StoreId = uint(fVal)
			case 4:
				fVal, err := strconv.ParseUint(fAcc.String(), 10, 32)
				if err != nil {
					return nil, err
				}
				result.InventorySourceId = uint(fVal)
			case 5:
				fVal, err := strconv.ParseUint(fAcc.String(), 10, 32)
				if err != nil {
					return nil, err
				}
				result.InventorySourceType = uint(fVal)
			case 6:
				result.IsVaries, _ = strconv.ParseBool(fAcc.String())
				break L
			}
			fIndex++
			fAcc.Reset()
			continue
		}
		fAcc.WriteByte(serialized[indx])
	}

	return result, nil
}

func ISPFullFromProductInventory(
	item dbinv.ProductInventory,
	inventorySourceType uint,
	inventorySourceId uint,
	storeId uint,
	isVaries bool,
) RProductInventoryFull {
	var result = RProductInventoryFull{
		InventorySourceType: inventorySourceType,
		InventorySourceId:   inventorySourceId,
		StoreId:             storeId,
		IsVaries:            isVaries,
	}

	var parent = ISPFromProductInventory(item)

	result.Quantity = parent.Quantity
	result.Price = parent.Price
	result.ValidDate = parent.ValidDate

	return result
}

func ISPSerialize(pr *RProductInventory) string {
	return strconv.FormatUint(pr.Price, 10) + "|" + strconv.FormatUint(uint64(pr.Quantity), 10) + "|" + pr.ValidDate + "|"
}

func ISPUnserialize(serialized string) (*RProductInventory, error) {
	var result = &RProductInventory{}

	var fAcc strings.Builder
	var fIndex = 0
L:
	for indx := range serialized {
		if serialized[indx] == '|' {
			switch fIndex {
			case 0:
				fVal, err := strconv.ParseUint(fAcc.String(), 10, 64)
				if err != nil {
					return nil, err
				}
				result.Price = fVal
			case 1:
				fVal, err := strconv.Atoi(fAcc.String())
				if err != nil {
					return nil, err
				}
				result.Quantity = uint(fVal)
			case 2:
				result.ValidDate = fAcc.String()
				break L
			}
			fIndex++
			fAcc.Reset()
			continue
		}
		fAcc.WriteByte(serialized[indx])
	}

	return result, nil
}

func (rir *RedisInvRepository) importProductsHash(hashName string, items map[uint]string, truncate bool) error {
	oneTimeLimiter <- true
	defer func() {
		<-oneTimeLimiter
	}()

	var tx = rir.client.TxPipeline()

	if truncate {
		tx.Del(*rir.ctx, hashName)
	}

	for productId, resultItem := range items {
		if resultItem == "" {
			tx.HDel(*rir.ctx, hashName, strconv.FormatUint(uint64(productId), 10))
		} else {
			tx.HSet(*rir.ctx, hashName, productId, resultItem)
		}
	}

	_, err := tx.Exec(*rir.ctx)

	return err
}

func (rir *RedisInvRepository) getInventoryHashName(isId uint) string {
	return fmt.Sprintf("inventory:%d", isId)
}

func (rir *RedisInvRepository) getRegionInventoryHashName(regionId uint) string {
	return fmt.Sprintf("inventory-region:%d", regionId)
}

func (rir *RedisInvRepository) getRegionStInventoryHashName(regionId uint) string {
	return fmt.Sprintf("inventory-region-st:%d", regionId)
}

func (rir *RedisInvRepository) getRegionMaxInventoryHashName(regionId uint) string {
	return fmt.Sprintf("inventory-max:%d", regionId)
}

func (rir *RedisInvRepository) getRegionLastInventoryHashName(regionId uint) string {
	return fmt.Sprintf("inventory-last:%d", regionId)
}

func (rir *RedisInvRepository) GetInventory(productId uint, isId uint) (*RProductInventory, error) {
	var hashName = rir.getInventoryHashName(isId)

	resRaw, err := rir.client.HGet(*rir.ctx, hashName, strconv.FormatUint(uint64(productId), 10)).Result()
	if err != nil {
		return nil, err
	}

	result, err := ISPUnserialize(resRaw)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (rir *RedisInvRepository) GetRegionInventory(regionId uint, productId uint) ([]*RProductInventoryFull, error) {
	var hashName = rir.getRegionInventoryHashName(regionId)

	resRaw, err := rir.client.HGet(*rir.ctx, hashName, strconv.FormatUint(uint64(productId), 10)).Result()
	if err != nil {
		return nil, err
	}

	result, err := ISPFullCollUnSerialize(resRaw)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (rir *RedisInvRepository) GetRegionStCntInventory(regionId uint, productId uint) (uint, error) {
	var hashName = rir.getRegionStInventoryHashName(regionId)

	resRaw, err := rir.client.HGet(*rir.ctx, hashName, strconv.FormatUint(uint64(productId), 10)).Result()
	if err != nil {
		return 0, err
	}

	result, err := ISPFullStCollUnSerialize(resRaw)
	if err != nil {
		return 0, err
	}

	return result, nil
}

func (rir *RedisInvRepository) GetRegionMaxInventory(regionId uint, productId uint) (*RProductInventoryFull, error) {
	var hashName = rir.getRegionMaxInventoryHashName(regionId)

	resRaw, err := rir.client.HGet(*rir.ctx, hashName, strconv.FormatUint(uint64(productId), 10)).Result()
	if err != nil {
		return nil, err
	}

	result, err := ISPFullUnserialize(resRaw)
	if err != nil {
		return nil, err
	}

	return result, nil
}


func (rir *RedisInvRepository) GetRegionLastInventory(regionId uint, productId uint) (*RProductInventoryFull, error) {
	var hashName = rir.getRegionLastInventoryHashName(regionId)

	resRaw, err := rir.client.HGet(*rir.ctx, hashName, strconv.FormatUint(uint64(productId), 10)).Result()
	if err != nil {
		return nil, err
	}

	result, err := ISPFullUnserialize(resRaw)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (rir *RedisInvRepository) UpdateInventorySource(isId uint, items map[uint]RProductInventory) error {
	var hashName = rir.getInventoryHashName(isId)

	var preparedItems = make(map[uint]string, len(items))

	for productId, item := range items {
		preparedItems[productId] = ISPSerialize(&item)
	}

	return rir.importProductsHash(hashName, preparedItems, true)
}

func (rir *RedisInvRepository) UpdateRegionInventory(regionId uint, items map[uint][]RProductInventoryFull) error {
	if len(items) == 0 {
		return nil
	}

	var hashName = rir.getRegionInventoryHashName(regionId)

	var preparedItems = make(map[uint]string, len(items))

	for productId, productsIspFull := range items {
		preparedItems[productId] = ISPFullCollSerialize(productsIspFull)
	}

	if err := rir.importProductsHash(hashName, preparedItems, false); err != nil {
		return err
	}

	return nil
}

func (rir *RedisInvRepository) UpdateRegionStInventory(regionId uint, items map[uint][]uint) error {
	if len(items) == 0 {
		return nil
	}

	var hashName = rir.getRegionStInventoryHashName(regionId)

	var preparedItems = make(map[uint]string, len(items))

	for productId, productsIspFull := range items {
		preparedItems[productId] = ISPFullStCollSerialize(productsIspFull)
	}

	if err := rir.importProductsHash(hashName, preparedItems, false); err != nil {
		return err
	}

	return nil
}

func (rir *RedisInvRepository) UpdateRegionMaxInventory(regionId uint, items map[uint]RProductInventoryFull) error {
	if len(items) == 0 {
		return nil
	}

	var hashName = rir.getRegionMaxInventoryHashName(regionId)

	var preparedItems = make(map[uint]string, len(items))

	for productId, productsIspFull := range items {
		preparedItems[productId] = ISPFullSerialize(&productsIspFull)
	}

	return rir.importProductsHash(hashName, preparedItems, false)
}

func (rir *RedisInvRepository) UpdateRegionLastInventory(regionId uint, items map[uint]RProductInventoryFull) error {
	if len(items) == 0 {
		return nil
	}

	var hashName = rir.getRegionLastInventoryHashName(regionId)

	var preparedItems = make(map[uint]string, len(items))

	for productId, productsIspFull := range items {
		preparedItems[productId] = ISPFullSerialize(&productsIspFull)
	}

	return rir.importProductsHash(hashName, preparedItems, false)
}

func (rir *RedisInvRepository) TruncateRegionInventory(regionId uint) {
	rir.client.Del(*rir.ctx, rir.getRegionInventoryHashName(regionId))
	rir.client.Del(*rir.ctx, rir.getRegionStInventoryHashName(regionId))
	rir.client.Del(*rir.ctx, rir.getRegionMaxInventoryHashName(regionId))
}

func (rir *RedisInvRepository) GetRegionInventoryPrId(regionId uint) ([]uint, error) {
	return rir.getRegionInventoryPrIdByHash(rir.getRegionInventoryHashName(regionId))
}

func (rir *RedisInvRepository) GetRegionStInventoryPrId(regionId uint) ([]uint, error) {
	return rir.getRegionInventoryPrIdByHash(rir.getRegionStInventoryHashName(regionId))
}

func (rir *RedisInvRepository) GetRegionMaxInventoryPrId(regionId uint) ([]uint, error) {
	return rir.getRegionInventoryPrIdByHash(rir.getRegionMaxInventoryHashName(regionId))
}

func (rir *RedisInvRepository) getRegionInventoryPrIdByHash(hashName string) ([]uint, error) {

	productIds, err := rir.client.HKeys(*rir.ctx, hashName).Result()
	if err != nil {
		return nil, err
	}

	var productIdsPrepared []uint

	for _, productId := range productIds {
		item, err := strconv.ParseUint(productId, 10, 32)
		if err != nil {
			return nil, err
		}

		productIdsPrepared = append(productIdsPrepared, uint(item))
	}

	return productIdsPrepared, nil
}

func (rir *RedisInvRepository) DeleteRegionInventory(regionId uint, productIds []uint) error {
	return rir.DeleteRegionInventoryByHash(rir.getRegionInventoryHashName(regionId), productIds)
}

func (rir *RedisInvRepository) DeleteRegionStInventory(regionId uint, productIds []uint) error {
	return rir.DeleteRegionInventoryByHash(rir.getRegionStInventoryHashName(regionId), productIds)
}

func (rir *RedisInvRepository) DeleteRegionMaxInventory(regionId uint, productIds []uint) error {
	return rir.DeleteRegionInventoryByHash(rir.getRegionMaxInventoryHashName(regionId), productIds)
}

func (rir *RedisInvRepository) DeleteRegionInventoryByHash(hashName string, productIds []uint) error {
	if len(productIds) == 0 {
		return nil
	}

	oneTimeLimiter <- true
	defer func() {
		<-oneTimeLimiter
	}()

	var tx = rir.client.TxPipeline()

	for _, productId := range productIds {
		tx.HDel(*rir.ctx, hashName, strconv.FormatUint(uint64(productId), 10))
	}

	_, err := tx.Exec(*rir.ctx)

	return err
}

func GetRepository(ctx *context.Context, client *redis.Client) RedisInvContract {
	return &RedisInvRepository{
		client: client,
		ctx:    ctx,
	}
}

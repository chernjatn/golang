package dbinv

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"main/internal/ecom"
	"main/internal/types"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const MYSQL_TIMEFORMAT = "2006-01-02 15:04:05"

const connectionLimit = 20

var oneTimeLimiter = make(chan bool, 1)
var rateLimiter = rate.NewLimiter(rate.Limit(connectionLimit), connectionLimit+1)

type ProductInventory struct {
	Id                string         `db:"id"`
	ProductId         uint           `db:"product_id"`
	InventorySourceId uint           `db:"inventory_source_id"`
	Qty               types.Quantity `db:"qty"`
	Price             types.Price    `db:"price"`
	ValidDate         string         `db:"valid_date"`
}

type InventorySource struct {
	Id          uint         `db:"id"`
	RegionId    uint         `db:"region_id"`
	ChangedDate sql.NullTime `db:"changed_at"`
}

type RegionInventorySource struct {
	Id      uint `db:"inventory_source_id"`
	StoreId uint `db:"store_id"`
	Type    uint `db:"type"`
}

type Discount struct {
	Type      string `db:"type"`
	TypeValue uint   `db:"type_value"`
}

type RegionProductDiscount = map[uint]map[uint]Discount

type ProductInventoryIterator struct {
	db        *sql.DB
	ctx       *context.Context
	condition string
	chunkSize uint
	curOffset uint
	curIndx   uint
	items     []*ProductInventory
	Err       error
}

func prepareInStatement(field string, vals []uint, notIn bool) string {
	var condition strings.Builder

	condition.WriteByte(' ')
	condition.WriteString(field)

	if notIn {
		condition.WriteString(" NOT IN(")
	} else {
		condition.WriteString(" IN(")
	}

	for indx, val := range vals {
		if indx != 0 {
			condition.WriteByte(',')
		}

		condition.WriteString(prepareUint(val))
	}

	condition.WriteByte(')')

	return condition.String()
}

func prepareInStatementStr(field string, vals []string, notIn bool) string {
	var condition strings.Builder

	condition.WriteByte(' ')
	condition.WriteString(field)

	if notIn {
		condition.WriteString(" NOT IN(")
	} else {
		condition.WriteString(" IN(")
	}

	for indx, val := range vals {
		if indx != 0 {
			condition.WriteByte(',')
		}

		condition.WriteByte('"')
		condition.WriteString(val)
		condition.WriteByte('"')
	}

	condition.WriteByte(')')

	return condition.String()
}

func prepareUint(val uint) string {
	return strconv.FormatUint(uint64(val), 10)
}

func (iterator *ProductInventoryIterator) HasNext() bool {
	if int(iterator.curIndx) == len(iterator.items) {

		var condition strings.Builder

		iterator.curOffset = iterator.curOffset + iterator.curIndx
		iterator.curIndx = 0
		iterator.items = nil

		condition.WriteString("SELECT id, inventory_source_id, product_id, qty, price * 100, valid_date FROM product_inventories WHERE")
		condition.WriteString(iterator.condition)
		condition.WriteString(" LIMIT ")
		condition.WriteString(prepareUint(iterator.chunkSize))

		if iterator.curOffset > 0 {
			condition.WriteString(" OFFSET ")
			condition.WriteString(prepareUint(uint(iterator.curOffset)))
		}

		oneTimeLimiter <- true
		ctx, cancel := context.WithTimeout(*iterator.ctx, 180*time.Second)
		defer cancel()

		rows, err := iterator.db.QueryContext(ctx, condition.String())
		if err != nil {
			iterator.Err = err
			return false
		}
		defer rows.Close()
		<-oneTimeLimiter

		for rows.Next() {
			var productInv = &ProductInventory{}
			var validDate sql.NullString

			if err := rows.Scan(&productInv.Id, &productInv.InventorySourceId, &productInv.ProductId, &productInv.Qty, &productInv.Price, &validDate); err != nil {
				iterator.Err = err
				return false
			}

			if validDate.Valid {
				productInv.ValidDate = validDate.String[:10]
			}

			iterator.items = append(iterator.items, productInv)
		}

		if len(iterator.items) == 0 {
			return false
		}
	}

	return true
}

func (iterator *ProductInventoryIterator) GetNext() ProductInventory {
	defer func() {
		iterator.curIndx++
	}()

	return *iterator.items[iterator.curIndx]
}

type InventoryRepositoryContract interface {
	GetRegions() ([]uint, error)

	GetInventorySources(regionId uint) ([]InventorySource, error)
	GetNoPickupInventorySources(regionId uint) (map[uint]uint, error)
	GetDarkstoresIds(regionId uint) (map[uint]uint, error)
	GetInventorySourcesStore(regionId uint) (map[uint][]RegionInventorySource, error)
	UpdateIScChangedAt(isId uint, changedAt *time.Time) error

	TruncateStocks(isId uint) error
	DeleteStocks(isId uint, productIds []uint) error
	UpsertStocks(isId uint, inventory map[uint]ecom.EcomProductInventory) error
	UpdateProductStocksChanged(productsId []uint) error

	GetStocksProductId(isIds []uint, skipProductIds []uint) ([]uint, error)
	GetStocks(isIds []uint, productIds []uint, chunkSize uint) (*ProductInventoryIterator, error)

	GetDiscounts() (RegionProductDiscount, error)
}

type inventoryRepository struct {
	db              *sql.DB
	ctx             *context.Context
	productsOnFetch chan bool
}

func (ir *inventoryRepository) getProductInventoryId(isId uint, productId uint, product ecom.EcomProductInventory) string {
	return fmt.Sprintf("%d-%d-%d-%d", isId, productId, product.Quantity.Int, product.EcomPrice.Int)
}

func (ir *inventoryRepository) GetRegions() ([]uint, error) {
	rateLimiter.Wait(*ir.ctx)
	ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
	defer cancel()
	rows, err := ir.db.QueryContext(ctx, "SELECT id from regions WHERE status=1")

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var regionIds []uint

	for rows.Next() {
		var regionId uint

		err := rows.Scan(&regionId)
		if err != nil {
			return nil, err
		}

		regionIds = append(regionIds, regionId)
	}

	return regionIds, nil
}

func (ir *inventoryRepository) GetInventorySources(regionId uint) ([]InventorySource, error) {
	rateLimiter.Wait(*ir.ctx)
	ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
	defer cancel()
	rows, err := ir.db.QueryContext(ctx, "SELECT id, region_id, changed_at from inventory_sources WHERE status=1 AND region_id="+prepareUint(regionId))

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []InventorySource

	for rows.Next() {
		var item InventorySource

		err := rows.Scan(&item.Id, &item.RegionId, &item.ChangedDate)
		if err != nil {
			return nil, err
		}

		items = append(items, item)
	}

	return items, nil
}

func (ir *inventoryRepository) GetNoPickupInventorySources(regionId uint) (map[uint]uint, error) {
	rateLimiter.Wait(*ir.ctx)
	ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
	defer cancel()

	query := "SELECT inventory_source_id from stores" +
		" INNER JOIN store_inventory_source ON store_id=stores.id" +
		" WHERE pickup_point=0 AND status=1 AND region_id=?"

	rows, err := ir.db.QueryContext(ctx, query, regionId)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items = make(map[uint]uint, 0)

	for rows.Next() {
		var item uint

		err := rows.Scan(&item)
		if err != nil {
			return nil, err
		}

		items[item] = item
	}

	return items, nil
}

func (ir *inventoryRepository) GetDarkstoresIds(regionId uint) (map[uint]uint, error) {
	rateLimiter.Wait(*ir.ctx)
	ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
	defer cancel()

	query := "SELECT id from stores WHERE is_default=1 AND status=1 AND region_id=?"

	rows, err := ir.db.QueryContext(ctx, query, regionId)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items = make(map[uint]uint, 0)

	for rows.Next() {
		var item uint

		err := rows.Scan(&item)
		if err != nil {
			return nil, err
		}

		items[item] = item
	}

	return items, nil
}

func (ir *inventoryRepository) TruncateStocks(isId uint) error {
	rateLimiter.Wait(*ir.ctx)
	ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
	defer cancel()
	_, err := ir.db.ExecContext(ctx, "UPDATE inventory_sources SET changed_at=NULL WHERE id="+prepareUint(isId)+"; DELETE FROM product_inventories WHERE inventory_source_id="+prepareUint(isId))

	return err
}

func (ir *inventoryRepository) DeleteStocks(isId uint, productIds []uint) error {
	if len(productIds) == 0 {
		return nil
	}

	rateLimiter.Wait(*ir.ctx)
	ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
	defer cancel()
	_, err := ir.db.ExecContext(ctx, "DELETE FROM product_inventories WHERE inventory_source_id="+prepareUint(isId)+" AND"+prepareInStatement("product_id", productIds, false))

	return err
}

func (ir *inventoryRepository) UpdateIScChangedAt(isId uint, changedAt *time.Time) error {
	var changedAtStr = "NULL"

	if changedAt != nil && !changedAt.IsZero() {
		changedAtStr = "\"" + changedAt.Format(MYSQL_TIMEFORMAT) + "\""
	}

	rateLimiter.Wait(*ir.ctx)
	ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
	defer cancel()
	_, err := ir.db.ExecContext(ctx, "UPDATE inventory_sources SET changed_at="+changedAtStr+" WHERE id="+prepareUint(isId))

	return err
}

func (ir *inventoryRepository) UpdateProductStocksChanged(productsId []uint) error {
	if len(productsId) == 0 {
		return nil
	}

	const chunkSize = 5_000

	for i := 0; i < len(productsId); i += chunkSize {
		end := i + chunkSize
		if end > len(productsId) {
			end = len(productsId)
		}

		rateLimiter.Wait(*ir.ctx)
		ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
		defer cancel()
		_, err := ir.db.ExecContext(ctx, "UPDATE products SET stocks_changed=1 WHERE"+prepareInStatement("id", productsId[i:end], false))
		if err != nil {
			return err
		}
	}

	return nil
}

func (ir *inventoryRepository) UpsertStocks(isId uint, inventory map[uint]ecom.EcomProductInventory) error {
	var query = "INSERT INTO product_inventories(id, product_id, inventory_source_id, price, qty, valid_date) VALUES "
	var queryEnd = " on duplicate key update id=values(id), price=values(price), qty=values(qty), valid_date=values(valid_date);"

	var valuesCount = 0
	var valuesQuery = ""

	for productId, productInv := range inventory {
		valuesCount++

		var validDate = "NULL"

		if productInv.ValidDate != "" {
			validDate = "\"" + productInv.ValidDate + "\""
		}

		valuesQuery += fmt.Sprintf(
			`("%s", %d, %d, %s, %d, %s),`,
			ir.getProductInventoryId(isId, productId, productInv),
			productId,
			isId,
			productInv.EcomPrice.ToDecimal(),
			productInv.Quantity.Int,
			validDate,
		)

		if valuesCount > 1000 {
			rateLimiter.Wait(*ir.ctx)
			ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
			defer cancel()
			_, err := ir.db.ExecContext(ctx, query+valuesQuery[:len(valuesQuery)-1]+queryEnd)

			if err != nil {
				return err
			}
			valuesQuery = ""
			valuesCount = 0
		}
	}

	if valuesCount != 0 {
		rateLimiter.Wait(*ir.ctx)
		ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
		defer cancel()
		_, err := ir.db.ExecContext(ctx, query+valuesQuery[:len(valuesQuery)-1]+queryEnd)

		if err != nil {
			return err
		}
	}

	return nil
}

func (ir *inventoryRepository) GetStocksProductId(isIds []uint, skipProductIds []uint) ([]uint, error) {
	var query = "SELECT DISTINCT product_id FROM product_inventories"
	var condition = ""

	if len(isIds) != 0 {
		if len(isIds) == 1 {
			condition = " inventory_source_id=" + prepareUint(isIds[0])
		} else {
			condition += prepareInStatement("inventory_source_id", isIds, false)
		}
	}

	if len(skipProductIds) != 0 {
		if condition != "" {
			condition += " AND"
		}

		condition += prepareInStatement("product_id", skipProductIds, true)
	}

	if condition != "" {
		query += " WHERE" + condition
	}

	rateLimiter.Wait(*ir.ctx)
	ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
	defer cancel()
	rows, err := ir.db.QueryContext(ctx, query)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []uint

	for rows.Next() {
		var productId uint
		if err := rows.Scan(&productId); err != nil {
			return nil, err
		}
		result = append(result, productId)
	}

	return result, nil
}

func (ir *inventoryRepository) GetStocks(isIds []uint, productIds []uint, chunkSize uint) (*ProductInventoryIterator, error) {
	var condition = ""

	if len(isIds) != 0 {
		if len(isIds) == 1 {
			condition += " inventory_source_id=" + prepareUint(isIds[0])
		} else {
			condition += prepareInStatement("inventory_source_id", isIds, false)
		}
	}

	if len(productIds) != 0 {
		if condition != "" {
			condition += " AND"
		}

		condition += prepareInStatement("product_id", productIds, false)
	}

	if condition == "" {
		return nil, errors.New("no condition")
	}

	return &ProductInventoryIterator{
		db:        ir.db,
		chunkSize: chunkSize,
		curIndx:   0,
		curOffset: 0,
		condition: condition,
		ctx:       ir.ctx,
	}, nil
}

func (ir *inventoryRepository) GetInventorySourcesStore(regionId uint) (map[uint][]RegionInventorySource, error) {
	rateLimiter.Wait(*ir.ctx)
	ctx, cancel := context.WithTimeout(*ir.ctx, 180*time.Second)
	defer cancel()
	rows, err := ir.db.QueryContext(ctx, "SELECT inventory_source_id,store_id,type FROM inventory_source_store_region WHERE status=1 AND region_id="+prepareUint(regionId))

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result = make(map[uint][]RegionInventorySource)

	for rows.Next() {
		var item = RegionInventorySource{}
		if err := rows.Scan(&item.Id, &item.StoreId, &item.Type); err != nil {
			return nil, err
		}
		result[item.Id] = append(result[item.Id], item)
	}

	return result, nil
}

func (ir *inventoryRepository) GetDiscounts() (RegionProductDiscount, error) {
	rateLimiter.Wait(*ir.ctx)
	ctx, cancel := context.WithTimeout(*ir.ctx, 20*time.Second)
	defer cancel()
	rows, err := ir.db.QueryContext(
		ctx,
		`SELECT region_id, discountable_id, type, type_value
			FROM discount_discountable 
			LEFT JOIN discounts ON discounts.id = discount_discountable.discount_id
			LEFT JOIN discount_region ON discount_region.discount_id = discount_discountable.discount_id
			WHERE status=1 AND (date_from IS NULL OR DATE(date_from) <= CURDATE()) AND (date_to IS NULL OR DATE(date_to) >= CURDATE())
			ORDER BY sort DESC`,
	)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer rows.Close()

	var result = make(map[uint]map[uint]Discount, 0)

	for rows.Next() {
		var sqlRegionId sql.NullInt32
		var productId uint

		var item = Discount{}
		if err := rows.Scan(&sqlRegionId, &productId, &item.Type, &item.TypeValue); err != nil {
			return nil, err
		}

		var regionId = uint(sqlRegionId.Int32)

		if _, ok := result[regionId]; !ok {
			result[regionId] = make(map[uint]Discount, 0)
		}

		result[regionId][productId] = item
	}

	return result, nil
}

func GetRepository(ctx *context.Context, db *sql.DB) InventoryRepositoryContract {
	return &inventoryRepository{
		db:              db,
		ctx:             ctx,
		productsOnFetch: make(chan bool, 1),
	}
}

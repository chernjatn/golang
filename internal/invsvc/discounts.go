package invsvc

import (
	context "context"
	"main/internal/dbinv"
	sync "sync"

	status "google.golang.org/grpc/status"
)

type discountsvc struct {
	ir    dbinv.InventoryRepositoryContract
	items dbinv.RegionProductDiscount
	mx    sync.RWMutex
}

type DiscountCalcResult struct {
	Price         uint64
	DiscountPrice uint64
	Sum           uint64
	DiscountSum   uint64
}

func mul64(a, b uint64) (uint64, error) {
	if a == 0 || b == 0 {
		return 0, status.Error(404, "overflow")
	}
	c := a * b
	if (c < 0) == ((a < 0) != (b < 0)) {
		if c/b == a {
			return c, nil
		}
	}
	return 0, status.Error(404, "overflow")
}

func (d *discountsvc) syncDiscounts() {
	d.mx.Lock()
	defer d.mx.Unlock()

	d.items, _ = d.ir.GetDiscounts()
}

func (d *discountsvc) getDiscount(regionId uint, productId uint) *dbinv.Discount {
	d.mx.RLock()
	defer d.mx.RUnlock()

	if d.items == nil || regionId == 0 {
		return nil
	}

	regionDiscount, hasRegionDiscounts := d.items[regionId]
	if hasRegionDiscounts {
		productDiscount, hasProductDiscount := regionDiscount[productId]
		if hasProductDiscount {
			return &dbinv.Discount{Type: productDiscount.Type, TypeValue: productDiscount.TypeValue}
		}
	}

	regionDiscount, hasDiscounts := d.items[0]
	if hasDiscounts {
		productDiscount, hasProductDiscount := regionDiscount[productId]
		if hasProductDiscount {
			return &dbinv.Discount{Type: productDiscount.Type, TypeValue: productDiscount.TypeValue}
		}
	}

	return nil
}

func (d *discountsvc) getPrice(price uint64, regionId uint, productId uint) uint64 {
	discount := d.getDiscount(regionId, productId)
	if discount == nil {
		return price
	}

	var newPrice uint64

	if discount.Type == "1" {
		newPrice = uint64(float64(price) * float64(100-discount.TypeValue) / 100)
	} else if discount.Type == "2" {
		newPrice = price - uint64(discount.TypeValue*10000)
	} else if discount.Type == "13" {
		newPrice = uint64(discount.TypeValue * 10000)
	} else {
		return price
	}

	if newPrice <= 1 {
		return price
	}

	rem := newPrice % 10_000

	if rem > 0 {
		newPrice = newPrice - rem + 10_000
	}

	return uint64(newPrice)
}

func (d *discountsvc) calc(
	ctx context.Context,
	regionId uint,
	productId uint,
	quantity uint,
	price uint64,
) (*DiscountCalcResult, error) {
	var err error

	discountPrice := d.getPrice(price, regionId, productId)
	if (discountPrice > price) {
		price = discountPrice
	}
	sum := price
	discountSum := discountPrice

	if quantity > 1 {
		sum, err = mul64(price, uint64(quantity))
		if err != nil {
			return nil, err
		}

		discountSum, err = mul64(discountPrice, uint64(quantity))
		if err != nil {
			return nil, err
		}

		if sum < 1 || discountSum < 1 {
			return nil, err
		}
	}

	return &DiscountCalcResult{
		Price:         price,
		DiscountPrice: discountPrice,
		Sum:           sum,
		DiscountSum:   discountSum,
	}, nil
}

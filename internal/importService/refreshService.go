package importService

import (
	"main/internal/dbinv"
	"main/internal/redisinv"
	"main/internal/types"
	"net/http"
)

const AFTER_STIMP_CMD = "shop:after-stimport"

type refreshService struct {
	ir           dbinv.InventoryRepositoryContract
	redisc       redisinv.RedisInvContract
	shopFlushUrl string
}

func (is *refreshService) refreshInventorySource(isId uint) error {
	var items = make(map[uint]redisinv.RProductInventory)

	iterator, err := is.ir.GetStocks([]uint{isId}, nil, 10_000)
	if err != nil {
		return err
	}

	for iterator.HasNext() {
		var item = iterator.GetNext()

		items[item.ProductId] = redisinv.ISPFromProductInventory(item)
	}

	if iterator.Err != nil {
		return iterator.Err
	}

	err = is.redisc.UpdateInventorySource(isId, items)
	if err != nil {
		return iterator.Err
	}

	return nil
}

func (is *refreshService) refreshRegion(productsChanged []uint, regionId uint) error {
	if len(productsChanged) == 0 {
		return nil
	}

	inventorySources, err := is.ir.GetInventorySourcesStore(regionId)
	if err != nil {
		return err
	}

	noPickupInventorySources, err := is.ir.GetNoPickupInventorySources(regionId)
	if err != nil {
		return err
	}

	darkstoresIds, err := is.ir.GetDarkstoresIds(regionId)
	if err != nil {
		return err
	}

	if len(inventorySources) == 0 {
		is.redisc.TruncateRegionInventory(regionId)
		return nil
	}

	var regionIsIds []uint
	for isId := range inventorySources {
		regionIsIds = append(regionIsIds, isId)
	}

	for i := 0; i < len(productsChanged); i += 100 {
		var regionProducts = make(map[uint][]redisinv.RProductInventoryFull)
		var regionProductsSt = make(map[uint][]uint)
		var regionProductsMax = make(map[uint]redisinv.RProductInventoryFull)

		j := i + 100

		if j > len(productsChanged) {
			j = len(productsChanged)
		}

		stocksIterator, err := is.ir.GetStocks(regionIsIds, productsChanged[i:j], 5_000)
		if err != nil {
			return err
		}

		for stocksIterator.HasNext() {
			var stock = stocksIterator.GetNext()

			_, isNoPickupIS := noPickupInventorySources[stock.InventorySourceId]

			if storeInvs, found := inventorySources[stock.InventorySourceId]; found {
				for _, storeInv := range storeInvs {
					_, isDarkstore := darkstoresIds[storeInv.StoreId]

					//тут может быть всеже нужна проверка типа regionProducts[stock.ProductId][storeInv.StoreId][storeInv.Id]

					regionProducts[stock.ProductId] = append(regionProducts[stock.ProductId], redisinv.ISPFullFromProductInventory(
						stock,
						storeInv.Type,
						storeInv.Id,
						storeInv.StoreId,
						false,
					))

					if !isNoPickupIS && storeInv.Type == types.InventorySourceTypeMain {
						regionProductsSt[stock.ProductId] = append(regionProductsSt[stock.ProductId], storeInv.StoreId)
					}

					productRegionMax, found := regionProductsMax[stock.ProductId]
					if !found {
						maxISType := storeInv.Type
						if isDarkstore {
							maxISType = types.InventorySourceTypeDarkstore
						}

						regionProductsMax[stock.ProductId] = redisinv.ISPFullFromProductInventory(
							stock,
							maxISType,
							stock.InventorySourceId,
							storeInv.StoreId,
							false,
						)
					} else {
						changed := false

						// склад с самовывозом && главный склад+аптека &&
						// (старая запись это даркстор(чтобы перекрыть даркстор в любом случае) || кол-во больше чем старое)
						if !isNoPickupIS && storeInv.Type == types.InventorySourceTypeMain &&
							(productRegionMax.InventorySourceType != types.InventorySourceTypeMain ||
								stock.Qty.Int > productRegionMax.Quantity) {
							productRegionMax.Quantity = stock.Qty.Int
							productRegionMax.ValidDate = stock.ValidDate
							productRegionMax.StoreId = storeInv.StoreId
							productRegionMax.InventorySourceType = storeInv.Type
							productRegionMax.InventorySourceId = stock.InventorySourceId

							changed = true
						}

						if stock.Price.Int < productRegionMax.Price {
							productRegionMax.Price = stock.Price.Int
							productRegionMax.IsVaries = true

							changed = true
						}

						if !productRegionMax.IsVaries && stock.Price.Int != productRegionMax.Price {
							productRegionMax.IsVaries = true

							changed = true
						}

						if changed {
							regionProductsMax[stock.ProductId] = productRegionMax
						}
					}
				}
			}
		}

		if stocksIterator.Err != nil {
			return stocksIterator.Err
		}

		if err := is.redisc.UpdateRegionInventory(regionId, regionProducts); err != nil {
			return err
		}

		if err := is.redisc.UpdateRegionStInventory(regionId, regionProductsSt); err != nil {
			return err
		}

		if err := is.redisc.UpdateRegionMaxInventory(regionId, regionProductsMax); err != nil {
			return err
		}

		if err := is.redisc.UpdateRegionLastInventory(regionId, regionProductsMax); err != nil {
			return err
		}
	}

	dbStocks, err := func() (map[uint]uint, error) {
		// var hasPickupRegionIsIds = make([]uint, 0)
		// for _, regionIsId := range regionIsIds {
		// 	if _, isNoPickupIS := noPickupInventorySources[regionIsId]; isNoPickupIS {
		// 		continue
		// 	}

		// 	hasPickupRegionIsIds = append(hasPickupRegionIsIds, regionIsId)
		// }

		// items, err := is.ir.GetStocksProductId(hasPickupRegionIsIds, []uint{})
		items, err := is.ir.GetStocksProductId(regionIsIds, []uint{})
		if err != nil {
			return nil, err
		}

		var itemsMap = make(map[uint]uint, len(items))

		for _, item := range items {
			itemsMap[item] = item
		}

		return itemsMap, nil
	}()
	if err != nil {
		return err
	}

	for i := 0; i < 3; i++ {
		var stocksToDelete []uint

		var rdbStocks []uint
		var err error

		if i == 0 {
			rdbStocks, err = is.redisc.GetRegionInventoryPrId(regionId)
		} else if i == 1 {
			rdbStocks, err = is.redisc.GetRegionStInventoryPrId(regionId)
		} else {
			rdbStocks, err = is.redisc.GetRegionMaxInventoryPrId(regionId)
		}

		if err != nil {
			return err
		}

		for _, productId := range rdbStocks {
			if _, ok := dbStocks[productId]; !ok {
				stocksToDelete = append(stocksToDelete, productId)
			}
		}

		if len(stocksToDelete) == 0 {
			continue
		}

		if i == 0 {
			is.redisc.DeleteRegionInventory(regionId, stocksToDelete)
		} else if i == 1 {
			is.redisc.DeleteRegionStInventory(regionId, stocksToDelete)
		} else {
			is.redisc.DeleteRegionMaxInventory(regionId, stocksToDelete)
		}
	}

	return nil
}

func (is *refreshService) ClearWebCache() {
	http.Get(is.shopFlushUrl)
}

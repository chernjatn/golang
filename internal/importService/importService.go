package importService

import (
	"context"
	"fmt"
	"main/internal/dbinv"
	"main/internal/ecom"
	"main/internal/redisinv"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
)

const IMPORT_IS_TIMEOUT = 180 * time.Second

type ImportServiceContract interface {
	ImportInventory(*context.Context) *ImportResult
}

type ImportService struct {
	ir             dbinv.InventoryRepositoryContract
	ec             *ecom.EcomClient
	refreshService *refreshService
}

type ImportResultCommon struct {
	Start      time.Time
	End        time.Time
	Success    bool
	Message    string
	mx         sync.Mutex
	printMx    sync.Mutex
	printTimer *time.Timer
}

func (irc *ImportResultCommon) StarStr() string {
	if !irc.Start.IsZero() {
		return irc.Start.Format("01-02 15:04")
	}
	return "NO"
}

func (irc *ImportResultCommon) EndStr() string {
	if !irc.End.IsZero() {
		return irc.End.Format("01-02 15:04")
	}
	return "NO"
}

func (irc *ImportResultCommon) SuccessStr() string {
	return fmt.Sprintf("%t", irc.Success)
}

func (irc *ImportResultCommon) Started() {
	irc.mx.Lock()
	irc.Success = false
	irc.Start = time.Now()
	irc.mx.Unlock()
}

func (irc *ImportResultCommon) Ended() {
	irc.mx.Lock()
	irc.Success = true
	irc.End = time.Now()
	fmt.Print("->")
	irc.mx.Unlock()
}

func (irc *ImportResultCommon) IsEnded() bool {
	return !irc.End.IsZero()
}

func (irc *ImportResultCommon) Error(message string) {
	irc.mx.Lock()
	irc.Message = message
	irc.Success = false
	irc.End = time.Now()
	irc.mx.Unlock()

	fmt.Println(message)
}

type ImportResult struct {
	ImportResultCommon
	Regions map[uint]*ImportRegionResult
}

func (ir *ImportResult) Print(force bool) {
	ir.printMx.Lock()
	defer ir.printMx.Unlock()

	var printer = func() {
		data := [][]string{}

		var pendingCountAll = 0
		var fullUpdateCountAll = 0

		for regionId, regionResult := range ir.Regions {
			regionData := [][]string{}

			var pendingCount = 0
			var fullUpdateCount = 0

			for iscId, iscResult := range regionResult.Isc {
				regionData = append(regionData, []string{
					"-",
					strconv.FormatUint(uint64(iscId), 10),
					iscResult.SuccessStr(),
					iscResult.StarStr(),
					iscResult.EndStr(),
					fmt.Sprintf("%t", iscResult.FullUpdate),
					fmt.Sprintf("Products: %d\nHasStocks:%d\nEmptyStocks:%d", len(iscResult.Products), iscResult.HasStocks, iscResult.EmptyStocks),
					regionResult.Message,
				})

				if !iscResult.IsEnded() {
					pendingCount++
				}

				if iscResult.FullUpdate {
					fullUpdateCount++
				}
			}

			var result = func() string {
				if pendingCount != 0 {
					pendingCountAll += pendingCount
					return fmt.Sprintf("Pending %d", pendingCount)
				}
				return "Fin"
			}()

			fullUpdateCountAll += fullUpdateCount

			data = append(data, []string{
				strconv.FormatUint(uint64(regionId), 10),
				strconv.Itoa(len(regionResult.Isc)),
				regionResult.SuccessStr(),
				regionResult.StarStr(),
				regionResult.EndStr(),
				strconv.Itoa(fullUpdateCount),
				result,
				regionResult.Message,
			})

			data = append(data, regionData...)
		}

		ir.mx.Lock()

		// f, err := os.Create("last.result.log")
		// if err != nil {
		// 	return
		// }
		// defer f.Close()

		table := tablewriter.NewWriter(os.Stdout)
		table.SetRowLine(true)
		table.SetHeader([]string{"Region", "Isc", "Success", "Start", "End", "FullUpdate", "Result", "Message"})

		var result = func() string {
			if pendingCountAll != 0 {
				return fmt.Sprintf("Pending %d", pendingCountAll)
			}
			return "Fin"
		}()

		table.SetFooter([]string{
			"All",
			strconv.Itoa(len(ir.Regions)),
			fmt.Sprintf("%t", ir.Success),
			ir.StarStr(),
			ir.EndStr(),
			strconv.Itoa(fullUpdateCountAll),
			result,
			ir.Message,
		})

		table.AppendBulk(data)
		table.Render()

		ir.mx.Unlock()
	}

	if ir.printTimer != nil {
		ir.printTimer.Stop()
	}

	if force {
		printer()
	} else {
		ir.printTimer = time.AfterFunc(5*time.Second, printer)
	}
}

func (ir *ImportResult) GetProductsChanged() []uint {

	var productsChanged = make(map[uint]uint, 0)
	for _, importRegionResult := range ir.Regions {
		for _, productId := range importRegionResult.GetProductsChanged() {
			productsChanged[productId] = productId
		}
	}

	var result []uint
	for _, productId := range productsChanged {
		result = append(result, productId)
	}

	return result
}

func (ir *ImportResult) InitRegions(regionsId []uint) {
	ir.mx.Lock()

	ir.Regions = make(map[uint]*ImportRegionResult, len(regionsId))
	for _, regionId := range regionsId {
		ir.Regions[regionId] = &ImportRegionResult{
			importResult: ir,
		}
	}

	ir.mx.Unlock()
}

type ImportRegionResult struct {
	ImportResultCommon
	importResult *ImportResult
	Isc          map[uint]*ImportIsResult `json:"isc"`
}

func (ir *ImportRegionResult) GetProductsChanged() []uint {
	var productsChanged = make(map[uint]uint, 0)

	for _, importIscResul := range ir.Isc {
		for _, productId := range importIscResul.Products {
			productsChanged[productId] = productId
		}
	}

	var result []uint
	for _, productId := range productsChanged {
		result = append(result, productId)
	}

	return result
}
func (ir *ImportRegionResult) InitIsc() {
	ir.mx.Lock()
	ir.Isc = make(map[uint]*ImportIsResult, 0)
	ir.mx.Unlock()
}

func (ir *ImportRegionResult) AddIsc(iscItem dbinv.InventorySource) {
	ir.mx.Lock()
	ir.Isc[iscItem.Id] = &ImportIsResult{
		importResult: ir.importResult,
	}
	ir.mx.Unlock()
}

type ImportIsResult struct {
	ImportResultCommon
	importResult *ImportResult
	FullUpdate   bool
	EmptyStocks  int
	HasStocks    int
	Products     []uint
}

func (ir *ImportIsResult) setProductsChanged(productId []uint) {
	ir.mx.Lock()
	ir.Products = productId
	ir.mx.Unlock()
}

func (is *ImportService) ImportInventory(parentCtx *context.Context) *ImportResult {
	var importResult = &ImportResult{}

	importResult.Started()

	regionIds, err := is.ir.GetRegions()
	if err != nil {
		importResult.Error(fmt.Sprintf("Err ImportInventory: %v", err))
		return importResult
	}

	importResult.InitRegions(regionIds)

	var wg sync.WaitGroup
	for _, regionId := range regionIds {
		wg.Add(1)
		go is.importRegion(&wg, importResult.Regions[regionId], regionId)
	}
	wg.Wait()

	if err := is.ir.UpdateProductStocksChanged(importResult.GetProductsChanged()); err != nil {
		importResult.Error(fmt.Sprintf("Err UpdateProductStocksChanged: %v", err))
		return importResult
	}

	fmt.Println("\n<->")
	for _, regionId := range regionIds {
		var regionResult = importResult.Regions[regionId]

		for iscId, importIsResult := range regionResult.Isc {

			wg.Add(1)
			go func(iscId uint, importIsResult *ImportIsResult) {
				defer wg.Done()
				if err := is.refreshService.refreshInventorySource(iscId); err != nil {
					importIsResult.Error(err.Error())
				}
			}(iscId, importIsResult)
		}

		wg.Add(1)
		go func(regionId uint) {
			defer wg.Done()
			if err := is.refreshService.refreshRegion(regionResult.GetProductsChanged(), regionId); err != nil {
				regionResult.Error(err.Error())
			}
		}(regionId)
	}

	wg.Wait()

	is.refreshService.ClearWebCache()

	importResult.Ended()
	// importResult.Print(true)
	fmt.Println("")

	return importResult
}

func (is *ImportService) importRegion(parentWg *sync.WaitGroup, importRegionResult *ImportRegionResult, regionId uint) {
	defer parentWg.Done()

	importRegionResult.Started()

	if !is.importRegionDB(importRegionResult, regionId) {
		return
	}

	importRegionResult.Ended()

	// importRegionResult.importResult.Print(false)
}

func (is *ImportService) importRegionDB(importRegionResult *ImportRegionResult, regionId uint) bool {
	importRegionResult.InitIsc()

	isItems, err := is.ir.GetInventorySources(regionId)
	if err != nil {
		importRegionResult.Error(fmt.Sprintf("Err importRegion: %d %v", regionId, err))
		return false
	}

	if len(isItems) == 0 {
		importRegionResult.Ended()
		return true
	}

	isItemsChaneDate, err := is.ec.GetInventoryChangeDate()
	if err != nil {
		importRegionResult.Error(fmt.Sprintf("Err importRegionDB: %d %v", regionId, err))
		return false
	}

	var wg sync.WaitGroup

	for _, isItem := range isItems {
		var fullUpdate bool = true
		var newChangedDate *time.Time

		if isItemRemote, ok := isItemsChaneDate[isItem.Id]; ok {
			if isItem.ChangedDate.Valid {
				if isItemRemote.FullStocksDate.Unix() <= isItem.ChangedDate.Time.Unix() && isItemRemote.PartStocksDate.Unix() <= isItem.ChangedDate.Time.Unix() {
					continue
				}

				if isItemRemote.FullStocksDate.After(isItem.ChangedDate.Time) {
					newChangedDate = isItemRemote.FullStocksDate
				} else {
					fullUpdate = false
					newChangedDate = isItemRemote.PartStocksDate
				}
			} else {
				newChangedDate = isItemRemote.FullStocksDate
			}
		}

		importRegionResult.AddIsc(isItem)
		wg.Add(1)
		go is.ImportInventorySource(&wg, importRegionResult.Isc[isItem.Id], isItem, fullUpdate, newChangedDate)
	}

	wg.Wait()

	return true
}

func (is *ImportService) ImportInventorySource(parentWg *sync.WaitGroup, importIsResult *ImportIsResult, isItem dbinv.InventorySource, fullUpdate bool, newChangedAt *time.Time) {
	defer func() {
		if importIsResult.Success {
			is.ir.UpdateIScChangedAt(isItem.Id, newChangedAt)
		}

		parentWg.Done()
	}()

	onEmptyStocks := func() {
		if fullUpdate {
			if err := is.ir.TruncateStocks(isItem.Id); err != nil {
				importIsResult.Error("TruncateStocks" + err.Error())
			} else {
				importIsResult.Error("Empty stocks")
			}
			return
		}

		importIsResult.Ended()
	}

	importIsResult.Started()
	importIsResult.FullUpdate = fullUpdate

	var changedDate time.Time
	if !fullUpdate && isItem.ChangedDate.Valid {
		changedDate = isItem.ChangedDate.Time
	}

	stocks, err := is.ec.GetStocks(isItem.Id, changedDate)
	if err != nil {
		importIsResult.Error(fmt.Sprintf("Err ImportInventorySource2: %d %v", isItem.Id, err))
		return
	}

	if len(stocks) == 0 {
		onEmptyStocks()
		return
	}

	var emptyStocks []uint
	var hasStocks = make(map[uint]ecom.EcomProductInventory, len(stocks))

	for productId, product := range stocks {
		if product.Quantity.IsEmpty() || product.EcomPrice.IsEmpty() {
			emptyStocks = append(emptyStocks, productId)
			continue
		}
		hasStocks[productId] = product
	}

	if fullUpdate {
		var emptyDBStocks []uint
		var skipProductIds = emptyStocks
		for productId := range hasStocks {
			skipProductIds = append(skipProductIds, productId)
		}

		emptyDBStocks, err = is.ir.GetStocksProductId([]uint{isItem.Id}, skipProductIds)
		if err != nil {
			importIsResult.Error(fmt.Sprintf("Err ImportInventorySource: %d %v", isItem.Id, err))
			return
		}

		emptyStocks = append(emptyStocks, emptyDBStocks...)
	}

	if len(emptyStocks) != 0 {
		importIsResult.EmptyStocks = len(emptyStocks)
		if err := is.ir.DeleteStocks(isItem.Id, emptyStocks); err != nil {
			importIsResult.Error("1" + err.Error())
			return
		}
	}

	if len(hasStocks) != 0 {
		importIsResult.HasStocks = len(hasStocks)
		if err := is.ir.UpsertStocks(isItem.Id, hasStocks); err != nil {
			importIsResult.Error("2" + err.Error())
			return
		}
	}

	var productsChanged = emptyStocks
	for productId := range hasStocks {
		productsChanged = append(productsChanged, productId)
	}

	importIsResult.setProductsChanged(productsChanged)

	importIsResult.Ended()
}

func GetService(ir dbinv.InventoryRepositoryContract, ec *ecom.EcomClient, redisc redisinv.RedisInvContract, shopFlushUrl string) ImportServiceContract {
	return &ImportService{
		ir: ir,
		ec: ec,
		refreshService: &refreshService{
			redisc:       redisc,
			ir:           ir,
			shopFlushUrl: shopFlushUrl,
		},
	}
}

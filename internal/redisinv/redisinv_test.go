package redisinv

import (
	"main/internal/ecom"
	"testing"
	"time"
)

var cases = []RProductInventory{
	{
		Price:     99,
		Quantity:  88,
		ValidDate: time.Now().Format(ecom.TIME_FORMAT),
	},
	{
		Price:     0,
		Quantity:  0,
		ValidDate: "",
	},
	{
		Price:     0,
		Quantity:  0,
		ValidDate: "2006-04-03",
	},
}

func getCasesFull() []RProductInventoryFull {
	var casesFull = []RProductInventoryFull{
		{
			StoreId:             6003,
			InventorySourceId:   6004,
			InventorySourceType: 0,
		},
		{
			StoreId:             6003,
			InventorySourceId:   6004,
			InventorySourceType: 1,
		},
		{
			StoreId:             1,
			InventorySourceId:   0,
			InventorySourceType: 0,
		},
		{
			StoreId:             0,
			InventorySourceId:   1,
			InventorySourceType: 1,
		},
	}

	for indx, caseItem := range cases {
		casesFull[indx].Price = caseItem.Price
		casesFull[indx].Quantity = caseItem.Quantity
		casesFull[indx].ValidDate = caseItem.ValidDate
	}

	return casesFull
}

func TestSerialize(t *testing.T) {
	for _, item := range cases {
		var itemSerialized = ISPSerialize(&item)
		itemUnserialized, err := ISPUnserialize(itemSerialized)

		if err != nil {
			t.Error(err)
		}

		if itemUnserialized.Price != item.Price {
			t.Error("Fail Price", itemUnserialized.Price, item.Price)
		}

		if itemUnserialized.Quantity != item.Quantity {
			t.Error("Fail Quantity", itemUnserialized.Quantity, item.Quantity)
		}

		if itemUnserialized.ValidDate != item.ValidDate {
			t.Error("Fail ValidDate", itemUnserialized.ValidDate, item.ValidDate)
		}
	}
}

func TestFullSerialize(t *testing.T) {
	for _, item := range getCasesFull() {
		var itemSerialized = ISPFullSerialize(&item)
		itemUnserialized, err := ISPFullUnserialize(itemSerialized)

		if err != nil {
			t.Error(err)
		}

		if itemUnserialized.Price != item.Price {
			t.Error("Fail Price", itemUnserialized.Price, item.Price)
		}

		if itemUnserialized.Quantity != item.Quantity {
			t.Error("Fail Quantity", itemUnserialized.Quantity, item.Quantity)
		}

		if itemUnserialized.ValidDate != item.ValidDate {
			t.Error("Fail ValidDate", itemUnserialized.ValidDate, item.ValidDate)
		}
	}
}

func TestFullCollSerialize(t *testing.T) {
	var items = getCasesFull()

	var itemSerialized = ISPFullCollSerialize(items)

	if len(itemSerialized) == 0 {
		t.Error("serialized wrong")
	}

	itemUnserialized, err := ISPFullCollUnSerialize(itemSerialized)

	if err != nil {
		t.Error("unserialiser", err)
	}

	if len(itemUnserialized) != len(items) {
		t.Error("unserialiser len wrong")
	}
}

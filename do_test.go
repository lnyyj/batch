package batch

import (
	"fmt"
	"testing"
)

func Test_batchdo(t *testing.T) {
	bdo := NewBDo(WhithCallback(func(dos []interface{}) error {
		fmt.Printf("---->[%d][%+v]\r\n", len(dos), dos)
		return fmt.Errorf("commit error")
	}))
	errors := bdo.Erorr()
	go func() {
		for {
			select {
			case err := <-errors:
				fmt.Println("------->err: ", err)
			}
		}
	}()

	for i := 1; ; i++ {
		bdo.Add(i)
		// time.Sleep(100 * time.Millisecond)
	}
}

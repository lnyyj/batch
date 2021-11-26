package batch

import (
	"fmt"
	"time"
	"testing"
)

func Test_batchdo(t *testing.T) {
	bdo := NewBDo(WhithTimeout(3*time.Second),WhithCount(20), WhithCallback(func(dos []interface{}, doType KindTiggerEventType) error {
		fmt.Printf("---->[%d][%+v][%+v]\r\n", len(dos), dos,doType)
		// return fmt.Errorf("commit error")
		return nil
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
		time.Sleep(100 * time.Millisecond)
	}
}

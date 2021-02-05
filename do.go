package batch

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// BDo ...
type BDo struct {
	MaxCount, MaxTimeInv int // 计数提交/超时提交
	CB                   func(dos []interface{}) error

	dos    []interface{}
	chdos  chan []interface{}
	lock   sync.Mutex
	errs   chan error
	closed int32
}

// NewDo ...
func NewDo(args ...int) *BDo {
	bdo := &BDo{MaxCount: 1024, MaxTimeInv: 15}
	if len(args) == 2 {
		bdo.MaxCount, bdo.MaxTimeInv = args[0], args[1]
	}
	bdo.dos = make([]interface{}, 0, bdo.MaxCount/2)
	bdo.chdos = make(chan []interface{}, 10)

	go bdo.do()
	return bdo
}

func (b *BDo) Callback(cb func(dos []interface{}) error) *BDo {
	b.CB = cb
	return b
}
func (b *BDo) Erorr() (errs <-chan error) {
	b.errs = make(chan error)
	return b.errs
}

func (b *BDo) Add(v interface{}, flush ...bool) error {
	if b.IsDone() {
		return errors.New("is closed")
	}else if count := len(b.dos); count >= b.MaxCount || len(flush)>0{
		b.flush()
	}
	b.lock.Lock()
	b.dos = append(b.dos, v)
	b.lock.Unlock()

	return nil
}

func (b *BDo) IsDone() bool {
	return atomic.LoadInt32(&b.closed) == 1
}

func (b *BDo) Done() {
	if !b.IsDone() {
		atomic.SwapInt32(&b.closed, 1)
		b.flush()
	}
}

func (b *BDo) flush() {
	b.lock.Lock()
	defer b.lock.Unlock()
	if l := len(b.dos); l > 0 {
		b.chdos <- b.dos
		b.dos = make([]interface{}, 0, b.MaxCount/2)
	}
}

func (b *BDo) do() {
	for {
		select {
		case <-time.After(time.Duration(b.MaxTimeInv) * time.Second):
			if !b.IsDone() {
				b.flush()
			}

		case dos := <-b.chdos:
			if b.CB != nil {
				if err := b.CB(dos); err != nil {
					if b.errs != nil {
						b.errs <- err
					} else {
						logrus.Errorf("batch do error: " + err.Error())
					}
				}
			}
			if b.IsDone() {
				return
			}
		}
	}
}

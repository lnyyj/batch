package batch

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type BDoOption func(*BDoOptions)
type BDoCallback func([]interface{}) error

// BDoOptions 参数配置
type BDoOptions struct {
	Count   int           // 计数提交
	Timeout time.Duration // 超时提交
	CB      BDoCallback   // 提交时执行的回调函数
}

func WhithCount(c int) BDoOption {
	return func(o *BDoOptions) {
		o.Count = c
	}
}
func WhithTimeout(timeout time.Duration) BDoOption {
	return func(o *BDoOptions) {
		o.Timeout = timeout
	}
}

func WhithCallback(cb BDoCallback) BDoOption {
	return func(o *BDoOptions) {
		o.CB = cb
	}
}

// BDo ...
type BDo struct {
	cb      func(dos []interface{}) error
	count   int           // 计数提交
	timeout time.Duration // 每次提交的超时时间

	dos    []interface{}
	chdos  chan []interface{}
	lock   sync.Mutex
	errs   chan error
	closed int32
}

var defaultBDoOptions = &BDoOptions{Count: 1024, Timeout: 15}

// NewBDo ...
func NewBDo(ops ...BDoOption) *BDo {
	_ops := defaultBDoOptions
	for _, opFunc := range ops {
		opFunc(_ops)
	}
	bdo := &BDo{
		count:   _ops.Count,
		timeout: _ops.Timeout,
		cb:      _ops.CB,
	}
	bdo.dos = make([]interface{}, 0, bdo.count/2)
	bdo.chdos = make(chan []interface{}, 10)

	go bdo.do()
	return bdo
}

func (b *BDo) Erorr() (errs <-chan error) {
	b.errs = make(chan error)
	return b.errs
}

func (b *BDo) Add(v interface{}, flush ...bool) error {
	if b.IsDone() {
		return errors.New("is closed")
	} else if count := len(b.dos); count >= b.count || len(flush) > 0 {
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
		b.dos = make([]interface{}, 0, b.count/2)
	}
}

func (b *BDo) do() {
	for {
		select {
		case <-time.After(b.timeout):
			if !b.IsDone() {
				b.flush()
			}

		case dos := <-b.chdos:
			if b.cb != nil {
				if err := b.cb(dos); err != nil {
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

package batch

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type KindTiggerEventType int

const (
	KindTiggerEventTypeTime  KindTiggerEventType = 1
	KindTiggerEventTypeCount KindTiggerEventType = 2
	KindTiggerEventTypeForce KindTiggerEventType = 3
	KindTiggerEventTypeClose KindTiggerEventType = 10
)

type BDoOption func(*BDoOptions)
type BDoCallback func([]interface{}, KindTiggerEventType) error

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
	cb      func(dos []interface{}, doType KindTiggerEventType) error
	count   int           // 计数提交
	timeout time.Duration // 每次提交的超时时间

	dos    event
	chdos  chan event
	lock   sync.Mutex
	errs   chan error
	closed int32
}

type event struct {
	source    []interface{}
	eventType KindTiggerEventType
}

var defaultBDoOptions = &BDoOptions{Count: 1024, Timeout: 15 * time.Second}

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
	bdo.dos.source = make([]interface{}, 0, bdo.count/2)
	bdo.chdos = make(chan event, 128)

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
	} else if count := len(b.dos.source); count >= b.count {
		b.flush(KindTiggerEventTypeCount)
	} else if len(flush) > 0 {
		b.flush(KindTiggerEventTypeForce)
	}
	b.lock.Lock()
	b.dos.source = append(b.dos.source, v)
	b.lock.Unlock()

	return nil
}

func (b *BDo) IsDone() bool {
	return atomic.LoadInt32(&b.closed) == 1
}

func (b *BDo) Done() {
	if !b.IsDone() {
		atomic.SwapInt32(&b.closed, 1)
		b.flush(KindTiggerEventTypeClose)
	}
}

func (b *BDo) flush(eventType KindTiggerEventType) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if l := len(b.dos.source); l > 0 {
		b.dos.eventType = eventType
		b.chdos <- b.dos
		b.dos.source = make([]interface{}, 0, b.count/2)
	}
}

func (b *BDo) do() {
	tc := time.NewTicker(b.timeout)
	defer tc.Stop()
	for {
		select {
		case <-tc.C:
			if !b.IsDone() {
				b.flush(KindTiggerEventTypeTime)
			}

		case dos := <-b.chdos:
			if b.cb != nil {
				if err := b.cb(dos.source, dos.eventType); err != nil {
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

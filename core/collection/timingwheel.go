package collection

import (
	"container/list"
	"errors"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/threading"
	"github.com/zeromicro/go-zero/core/timex"
)

const drainWorkers = 8

// 构建一个时间轮，用于延迟任务的执行。
// 任务信息以链表的形式挂载到每一个slot上。 任务信息包含circle字段，用于表示时间轮需要转多少圈，才能执行本任务。（时间轮每转一圈，circle-1）
// 任务信息同时会存储再一个map中，TimingWheel.timers， 由于slot上挂载的是List，不便于做删除等操作。
// 所以在Map中也存储一份任务key对应的任务信息。方便做删除标记。

var (
	ErrClosed   = errors.New("TimingWheel is closed already")
	ErrArgument = errors.New("incorrect task argument")
)

type (
	// Execute defines the method to execute the task.
	Execute func(key, value any)

	// A TimingWheel is a timing wheel object to schedule tasks.
	TimingWheel struct {
		// 时间轮的精度。  时间轮精度 * 槽位数 = 时间轮一圈的时间。 interval * numSlots = time span in one cycle
		interval time.Duration
		ticker   timex.Ticker
		slots    []*list.List
		timers   *SafeMap
		// 指向时间轮中的当前节点， 每次定时器到期，会更新这个值。
		tickedPos int
		// slot的数量，每个slot会挂载一个List， 所有的List存储在slots字段上，通过索引下表获取
		numSlots int
		// 任务的执行函数。 时间轮中值存储了任务的key和val。 任务的执行用统一的函数execute来执行，key和val作为参数。
		execute       Execute
		setChannel    chan timingEntry
		moveChannel   chan baseEntry
		removeChannel chan any
		drainChannel  chan func(key, value any)
		stopChannel   chan lang.PlaceholderType
	}

	timingEntry struct {
		baseEntry
		value any
		// 当时间轮转完一圈时，表明经过了一个指定的时间。但是时间轮有精度，无论精度设置成多小，时间轮的一圈所对应的时间都不可能是无限的。
		// 如果我们任务的延迟时间，超过了时间轮转完一整圈所代表的时间段，那么我们需要让时间轮多转几圈，来代表我们的时间。
		// circle字段，就是代表圈数。
		circle  int
		diff    int
		removed bool
	}

	baseEntry struct {
		delay time.Duration
		key   any
	}

	positionEntry struct {
		pos  int
		item *timingEntry
	}

	// 任务结构体，任务时间到期后，构建此结构体，用于执行任务
	timingTask struct {
		key   any
		value any
	}
)

// NewTimingWheel returns a TimingWheel.
func NewTimingWheel(interval time.Duration, numSlots int, execute Execute) (*TimingWheel, error) {
	if interval <= 0 || numSlots <= 0 || execute == nil {
		return nil, fmt.Errorf("interval: %v, slots: %d, execute: %p",
			interval, numSlots, execute)
	}

	return NewTimingWheelWithTicker(interval, numSlots, execute, timex.NewTicker(interval))
}

// NewTimingWheelWithTicker returns a TimingWheel with the given ticker.
func NewTimingWheelWithTicker(interval time.Duration, numSlots int, execute Execute,
	ticker timex.Ticker) (*TimingWheel, error) {
	tw := &TimingWheel{
		interval: interval,
		ticker:   ticker,
		slots:    make([]*list.List, numSlots),
		timers:   NewSafeMap(),
		// 指向时间轮中的最后一个节点。 比如numSlots为10，则时间轮被分成了10份，[0-9], 则tickedPos=9,指向最后一个
		// 当定时器到期时，会将tickedPos+1 % slotNum。 所以，第一次定时器到期，指到0位置。
		tickedPos:     numSlots - 1, // at previous virtual circle
		execute:       execute,
		numSlots:      numSlots,
		setChannel:    make(chan timingEntry),
		moveChannel:   make(chan baseEntry),
		removeChannel: make(chan any),
		drainChannel:  make(chan func(key, value any)),
		stopChannel:   make(chan lang.PlaceholderType),
	}

	tw.initSlots()
	go tw.run()

	return tw, nil
}

// Drain drains all items and executes them.
func (tw *TimingWheel) Drain(fn func(key, value any)) error {
	select {
	case tw.drainChannel <- fn:
		return nil
	case <-tw.stopChannel:
		return ErrClosed
	}
}

// MoveTimer moves the task with the given key to the given delay.
func (tw *TimingWheel) MoveTimer(key any, delay time.Duration) error {
	if delay <= 0 || key == nil {
		return ErrArgument
	}

	select {
	case tw.moveChannel <- baseEntry{
		delay: delay,
		key:   key,
	}:
		return nil
	case <-tw.stopChannel:
		return ErrClosed
	}
}

// RemoveTimer removes the task with the given key.
func (tw *TimingWheel) RemoveTimer(key any) error {
	if key == nil {
		return ErrArgument
	}

	select {
	case tw.removeChannel <- key:
		return nil
	case <-tw.stopChannel:
		return ErrClosed
	}
}

// 添加一个延迟任务，key是任务的key， value是任务的关联数据。 delay是任务的延迟执行时间。

// SetTimer sets the task value with the given key to the delay.
func (tw *TimingWheel) SetTimer(key, value any, delay time.Duration) error {
	if delay <= 0 || key == nil {
		return ErrArgument
	}

	select {
	// 添加任务
	case tw.setChannel <- timingEntry{
		baseEntry: baseEntry{
			delay: delay,
			key:   key,
		},
		value: value,
	}:
		return nil
	case <-tw.stopChannel:
		// 已关闭
		return ErrClosed
	}
}

// Stop stops tw. No more actions after stopping a TimingWheel.
func (tw *TimingWheel) Stop() {
	close(tw.stopChannel)
}

// 将所有slot上挂载的所有任务，都做执行操作。
func (tw *TimingWheel) drainAll(fn func(key, value any)) {
	// 限制任务的并发数量为8
	runner := threading.NewTaskRunner(drainWorkers)
	// 遍历链表切片slots， 得到每个slot上挂载的链表
	for _, slot := range tw.slots {
		// 获取链表的头部节点， 然后执行头部节点中的任务，完成后删除头部节点。
		// 依次循环，直到链表为空。
		for e := slot.Front(); e != nil; {
			task := e.Value.(*timingEntry)
			next := e.Next()
			slot.Remove(e)
			e = next
			if !task.removed {
				// 并发运行
				runner.Schedule(func() {
					fn(task.key, task.value)
				})
			}
		}
	}
}

// 参数d 为任务的延迟时间。 本函数根据延迟时间，计算一个任务在时间轮上的位置，以及需要转的圈数。
func (tw *TimingWheel) getPositionAndCircle(d time.Duration) (pos, circle int) {
	// 延迟时间 / 时间轮的精度  = 时间轮执行此任务需要转动的步数
	steps := int(d / tw.interval)
	// 这里的计算公式，我一直没想明白，我理解的应该是把括号去掉。后来想明白了，原因是 tw.tickedPos + steps % tw.numSlots 得到的值可能比槽位数大。比如6 + 25 % 9 = 13， 此时需要再对槽位数做一次取余才行。13 % 9 = 4
	// 下面计算公式的原理是这样的， step相当于是从当前指针的位置需要走多少个槽位数，我们加上tw.tickedPos后，就变成了从指针0处，需要走多少个槽位数。
	pos = (tw.tickedPos + steps) % tw.numSlots
	// 这里需要好好想一下，为什么steps需要减一。以为当前圈是第0圈。
	// 比如 8个位置，步数为8步时，虽然要转一圈后才执行，但是此时的圈数应该是0。
	circle = (steps - 1) / tw.numSlots

	return
}

func (tw *TimingWheel) initSlots() {
	// 每个slot上挂一个链表
	for i := 0; i < tw.numSlots; i++ {
		tw.slots[i] = list.New()
	}
}

func (tw *TimingWheel) moveTask(task baseEntry) {
	val, ok := tw.timers.Get(task.key)
	if !ok {
		return
	}

	timer := val.(*positionEntry)
	if task.delay < tw.interval {
		// 该任务的延迟时间已经小于一个时间槽的长度了, 所以可以立即执行该任务。
		threading.GoSafe(func() {
			tw.execute(timer.item.key, timer.item.value)
		})
		return
	}

	// 如下的这块逻辑，是为了避免，由于更改延迟时间，而导致的频繁的从槽位所关联的List中，删除和添加节点, 而采取的一种方案.
	// 在延迟时间改变时，我们不直接删除List中的节点，而是记录新的延迟时间所在的节点和老节点之间的位置关系（diff）
	// 在老节点被触发执行的时候，根据diff再重新创建新节点，再加入到槽位上。
	// 一共分了3中情况：
	// 1.  。 此时当老的被执行时，只需要向后加一点即可。
	// 2.  。 和1的情况类似，只是计算diff的方式有点差别，同时circle也需要调整。

	// 根据新设置的延迟时间，计算pos 和circle。
	pos, circle := tw.getPositionAndCircle(task.delay)
	if pos >= timer.pos { // circle 可能大于0， 也可能等于0
		timer.item.circle = circle
		timer.item.diff = pos - timer.pos
	} else if circle > 0 { // pos < timer.pos && circle > 0
		circle--
		timer.item.circle = circle
		// 这个diff是新的位置，相对于老的位置的偏移量。
		timer.item.diff = tw.numSlots + pos - timer.pos
	} else { // pos < timer.pos && circle == 0
		// 当前圈就要执行的时候，重新构建这个任务，并把老任务标记为删除。
		timer.item.removed = true
		newItem := &timingEntry{
			baseEntry: task,
			value:     timer.item.value,
		}
		tw.slots[pos].PushBack(newItem)
		tw.setTimerPosition(pos, newItem)
	}
}

func (tw *TimingWheel) onTick() {
	// 更新时间轮指针的位置
	tw.tickedPos = (tw.tickedPos + 1) % tw.numSlots
	// 获取指针指向的槽所对应的任务链表
	l := tw.slots[tw.tickedPos]
	// 运行任务
	tw.scanAndRunTasks(l)
}

// 将一个任务标记为删除， 并从任务map中删除此任务。
// 值得注意的是， slot上的List中还有此任务，在遍历slot中的List的时候，发现removed=true会直接忽略，并从链表中删除。
func (tw *TimingWheel) removeTask(key any) {
	val, ok := tw.timers.Get(key)
	if !ok {
		return
	}

	timer := val.(*positionEntry)
	// 任务标记为删除
	timer.item.removed = true
	// 从map中删除（删除后，slot上的List中还有此任务，在遍历List的时候，发现removed=true会直接忽略，并从链表中删除。）
	tw.timers.Del(key)
}

func (tw *TimingWheel) run() {
	for {
		select {
		case <-tw.ticker.Chan():
			// 时间轮到了，需要执行某个时间轮下的任务
			tw.onTick()
		case task := <-tw.setChannel:
			// 添加新任务
			tw.setTask(&task)
		case key := <-tw.removeChannel:
			// 将任务标记为删除，后面任务到期后，会被忽略，并不会执行。
			tw.removeTask(key)
		case task := <-tw.moveChannel:
			// 移动任务。 根据任务的key，修改任务的延迟时间。
			tw.moveTask(task)
		case fn := <-tw.drainChannel:
			//执行每个slot上的每一个任务
			tw.drainAll(fn)
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimingWheel) runTasks(tasks []timingTask) {
	if len(tasks) == 0 {
		return
	}

	go func() {
		for i := range tasks {
			threading.RunSafe(func() {
				// 运行执行器
				tw.execute(tasks[i].key, tasks[i].value)
			})
		}
	}()
}

func (tw *TimingWheel) scanAndRunTasks(l *list.List) {
	// 获取需要处理的任务
	var tasks []timingTask

	for e := l.Front(); e != nil; {
		task := e.Value.(*timingEntry)
		if task.removed {
			// 任务已经被移除
			next := e.Next()
			l.Remove(e)
			e = next
			continue
		} else if task.circle > 0 {
			// 如果任务的延迟时间超过时间轮一圈所代表的时间，则减少圈数，此时并不执行任务
			task.circle--
			e = e.Next()
			continue
		} else if task.diff > 0 { // task.circle == 0 && task.diff > 0
			next := e.Next()
			// 从槽中删除任务
			l.Remove(e)
			// (tw.tickedPos+task.diff)%tw.numSlots
			// cannot be the same value of tw.tickedPos
			// 把任务加入到新的槽中
			pos := (tw.tickedPos + task.diff) % tw.numSlots
			tw.slots[pos].PushBack(task)
			tw.setTimerPosition(pos, task)
			task.diff = 0
			e = next
			continue
		}

		tasks = append(tasks, timingTask{
			key:   task.key,
			value: task.value,
		})
		next := e.Next()
		l.Remove(e)
		tw.timers.Del(task.key)
		e = next
	}

	// 异步并行执行任务
	tw.runTasks(tasks)
}

func (tw *TimingWheel) setTask(task *timingEntry) {
	if task.delay < tw.interval {
		// 任务的延迟时间，不能小于时间轮的转动最小单位
		task.delay = tw.interval
	}

	if val, ok := tw.timers.Get(task.key); ok {
		entry := val.(*positionEntry)
		entry.item.value = task.value
		tw.moveTask(task.baseEntry)
	} else {
		// 获取我在时间轮中的位置， 以及需要转的圈数
		pos, circle := tw.getPositionAndCircle(task.delay)
		task.circle = circle
		// 任务信息插入到对应槽位的List的尾部。
		tw.slots[pos].PushBack(task)
		// 任务信息更新到map中
		tw.setTimerPosition(pos, task)
	}
}

// 本函数用于想map中添加任务，或者更新已有任务
func (tw *TimingWheel) setTimerPosition(pos int, task *timingEntry) {
	if val, ok := tw.timers.Get(task.key); ok {
		// 如果任务已存在，则更新pos，以及任务数据
		timer := val.(*positionEntry)
		timer.item = task
		timer.pos = pos
	} else {
		// 任务不存在，插入到map中
		tw.timers.Set(task.key, &positionEntry{
			pos:  pos,
			item: task,
		})
	}
}

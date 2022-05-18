package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	queue "github.com/enriquebris/goconcurrentqueue"
)

var (
	randomNumbers        chan int64
	q1MaxLength          int64 = 0
	q2MaxLength          int64 = 0
	interruptedProcesses int64 = 0
	m1                         = sync.Mutex{}
	m2                         = sync.Mutex{}
)

const (
	minWorkTime     = time.Second
	maxWorkTime     = 6 * time.Second
	minGenerateTime = time.Second / 2
	maxGenerateTime = 3 * time.Second
)

type CPUProcess struct {
	ch       chan int
	workTime time.Duration
}

func (process *CPUProcess) SetChannel(ch chan int) {
	process.ch = ch
}

func (process *CPUProcess) GetWorkTime() time.Duration {
	return process.workTime
}

func (process *CPUProcess) Work(ctx context.Context) {
	defer func() {
		process.ch <- 0
	}()
	select {
	case <-time.After(process.workTime):
		fmt.Println("done")
	case <-ctx.Done():
		fmt.Println("halted operation")
	}
}

type CPU func(ctx context.Context, wg sync.WaitGroup)

func NumberGenerator(ch chan int64) {
	source := rand.NewSource(time.Now().UnixNano())
	generator := rand.New(source)
	for {
		ch <- generator.Int63()
	}
}

func ProcessGenerator(ctx context.Context, firstQueue, secondQueue *queue.FIFO, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			goto outOfLoop
		case <-time.After(time.Duration(<-randomNumbers)%maxGenerateTime + minGenerateTime):
			process := CPUProcess{
				workTime: time.Duration(<-randomNumbers)%maxWorkTime + minWorkTime,
			}

			if <-randomNumbers%2 == 0 {
				m1.Lock()
				err := firstQueue.Enqueue(process)
				if err != nil {
					m1.Unlock()
					continue
				}
				if firstQueue.GetLen() > int(q1MaxLength) {
					atomic.StoreInt64(&q1MaxLength, int64(firstQueue.GetLen()))
				}
				fmt.Println("Generated process 1, size q1: ", firstQueue.GetLen())
				m1.Unlock()
			} else {
				m2.Lock()
				err := secondQueue.Enqueue(process)
				if err != nil {
					m2.Unlock()
					continue
				}
				if secondQueue.GetLen() > int(q2MaxLength) {
					atomic.StoreInt64(&q2MaxLength, int64(secondQueue.GetLen()))
				}
				fmt.Println("Generated process 2, size q2: ", secondQueue.GetLen())
				m2.Unlock()
			}
		}
	}
outOfLoop:
}

func main() {
	wg := &sync.WaitGroup{}

	//creating random numbers generator routine
	randomNumbers = make(chan int64)
	go NumberGenerator(randomNumbers)

	//creating concurrentsafe queues
	q1 := queue.NewFIFO()
	q2 := queue.NewFIFO()

	//creating process generator
	genctx := context.Background()
	genctx, gencancel := context.WithCancel(genctx)
	go ProcessGenerator(genctx, q1, q2, wg)

	//creating CPU1
	cpu1ctx := context.Background()
	cpu1ctx, cpu1cancel := context.WithCancel(cpu1ctx)
	cpu1 := func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		wg.Add(1)
		proc1chan := make(chan CPUProcess)
		proc2chan := make(chan CPUProcess)
		waitchan := make(chan int)

		for {
			var process CPUProcess
			prctx := context.Background()
			prctx, prcancel := context.WithCancel(prctx)

			deq1ctx := context.Background()
			deq1ctx, deq1cancel := context.WithCancel(deq1ctx)
			go func(ctx context.Context) {
				process, err := q1.DequeueOrWaitForNextElementContext(ctx)
				if err != nil {
					return
				}
				proc1chan <- process.(CPUProcess)
			}(deq1ctx)

			deq2ctx := context.Background()
			deq2ctx, deq2cancel := context.WithCancel(deq2ctx)
			go func(ctx context.Context) {
				process, err := q2.DequeueOrWaitForNextElementContext(ctx)
				if err != nil {
					return
				}
				proc2chan <- process.(CPUProcess)
			}(deq2ctx)

			select {
			case process = <-proc1chan:
				deq2cancel()
				fmt.Println("cpu1 got process from q1", "worktime: ", process.GetWorkTime())
				process.SetChannel(waitchan)
				go process.Work(prctx)

				select {
				case <-waitchan:
					fmt.Println("cpu1 finished processing")
				case <-ctx.Done():
					prcancel()
					<-waitchan
					fmt.Println("stopped cpu1")
					return
				}

			case process = <-proc2chan:
				fmt.Println("cpu1 got process from q2", "worktime: ", process.GetWorkTime())
				process.SetChannel(waitchan)
				go process.Work(prctx)
				var tmp CPUProcess
				select {
				case <-waitchan:
					fmt.Println("cpu1 finished processing")
				case tmp = <-proc1chan:
					prcancel()
					<-waitchan
					prctx, prcancel = context.WithCancel(prctx)
					atomic.AddInt64(&interruptedProcesses, 1)
					m2.Lock()
					q2.Enqueue(process)
					if q2.GetLen() > int(q2MaxLength) {
						atomic.StoreInt64(&q2MaxLength, int64(q2.GetLen()))
					}
					fmt.Println("size q2: ", q2.GetLen())
					m2.Unlock()
					process = tmp
					fmt.Println("cpu1 return process to q2")
					fmt.Println("cpu1 got process from q1", "worktime: ", process.GetWorkTime())
					process.SetChannel(waitchan)
					go process.Work(prctx)

					select {
					case <-waitchan:
						fmt.Println("cpu1 finished processing")
					case <-ctx.Done():
						prcancel()
						<-waitchan
						fmt.Println("stopped cpu1")
						return
					}
				case <-ctx.Done():
					prcancel()
					deq1cancel()
					deq2cancel()
					<-waitchan
					fmt.Println("stopped cpu1")
					return
				}
			case <-ctx.Done():
				deq1cancel()
				deq2cancel()
				prcancel()
				fmt.Println("stopped cpu1")
				return
			}
		}
	}

	//creating CPU2
	cpu2ctx := context.Background()
	cpu2ctx, cpu2cancel := context.WithCancel(cpu2ctx)
	cpu2 := func(ctx context.Context, wg *sync.WaitGroup) {
		defer wg.Done()
		wg.Add(1)
		procchan := make(chan CPUProcess)
		waitchan := make(chan int)
		for {
			var process CPUProcess
			prctx := context.Background()
			prctx, prcancel := context.WithCancel(prctx)

			deqctx := context.Background()
			deqctx, deqcancel := context.WithCancel(deqctx)
			go func(ctx context.Context) {
				process, err := q2.DequeueOrWaitForNextElementContext(ctx)
				if err != nil {
					return
				}
				procchan <- process.(CPUProcess)
			}(deqctx)

			select {
			case process = <-procchan:
				fmt.Println("cpu2 got process. ", "worktime: ", process.GetWorkTime())
				process.SetChannel(waitchan)
				go process.Work(prctx)
			case <-ctx.Done():
				fmt.Println("stopped cpu2")
				deqcancel()
				prcancel()
				return
			}

				select {
				case <-waitchan:
					fmt.Println("cpu2 finished processing")
				case <-ctx.Done():
					prcancel()
					<-waitchan
					fmt.Println("stopped cpu2")
					return
				}
		}
	}

	go cpu1(cpu1ctx, wg)
	go cpu2(cpu2ctx, wg)
	time.Sleep(time.Duration(30 * time.Second))
	gencancel()
	cpu1cancel()
	cpu2cancel()
	wg.Wait()

	fmt.Println("First queue max length: ", q1MaxLength)
	fmt.Println("Second queue max length: ", q2MaxLength)
	fmt.Println("Second queue processes interrupted: ", interruptedProcesses)
}

package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Generator генерирует последовательность чисел 1,2,3 и т.д. и
// отправляет их в канал ch. При этом после записи в канал для каждого числа
// вызывается функция fn. Она служит для подсчёта количества и суммы
// сгенерированных чисел.
func Generator(ctx context.Context, ch chan<- int64, fn func(int64)) {
	// 1. Функция Generator
	defer close(ch)
	var n int64 = 0
	for {
		select {
		case <-ctx.Done():
			//fmt.Println("Генерация завершилась")
			return
		default:
		}
		n++
		select {
		case <-ctx.Done():
			return
		case ch <- n:
			fn(n)
		}
		time.Sleep(time.Millisecond * 1)
	}
}

// Worker читает число из канала in и пишет его в канал out.
func Worker(in <-chan int64, out chan<- int64) {
	// 2. Функция Worker
	// получаю числа из канала и записываю в другой
	defer close(out)
	for num := range in {
		out <- num
	}
}

func main() {
	chIn := make(chan int64) // это канал для чисел из generator

	// 3. Создание контекста

	parentContext, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	// для проверки будем считать количество и сумму отправленных чисел
	var inputSum int64   // сумма сгенерированных чисел
	var inputCount int64 // количество сгенерированных чисел n из Generator
	var mu sync.Mutex

	// генерируем числа, считая параллельно их количество и сумму
	go Generator(parentContext, chIn, func(i int64) {
		mu.Lock()
		inputSum += i
		inputCount++ // посчитаем сколько чисел сгенерировал Generator
		mu.Unlock()
	})

	const NumOut = 5 // количество обрабатывающих горутин и каналов
	// outs — слайс каналов, куда будут записываться числа из chIn
	outs := make([]chan int64, NumOut)
	var workerWG sync.WaitGroup

	for i := 0; i < NumOut; i++ {
		// создаём каналы и для каждого из них вызываем горутину Worker
		outs[i] = make(chan int64)
		workerWG.Add(1)
		go func(workerIdx int, in <-chan int64, out chan<- int64) {
			defer workerWG.Done()
			Worker(in, out)
		}(i, chIn, outs[i])
	}

	// amounts — слайс, в который собирается статистика по горутинам
	amounts := make([]int64, NumOut) // количество чисел, обработанных каждым сборщиком
	// chOut — канал, в который будут отправляться числа из горутин `outs[i]`
	chOut := make(chan int64, NumOut)

	var wg sync.WaitGroup

	// 4. Собираем числа из каналов outs (Сборщики)

	for i := 0; i < NumOut; i++ {
		wg.Add(1)
		go func(idx int, outChan <-chan int64) {
			defer wg.Done()
			for {
				select {
				case <-parentContext.Done():
					return
				case num, ok := <-outChan: //  Читаем из канала воркера.
					if !ok {
						return
					}

					select {
					case <-parentContext.Done():
						return
					case chOut <- num:

						amounts[idx]++
					}
				}
			}
		}(i, outs[i])
	}

	go func() {
		// ждём завершения работы всех горутин для outs
		wg.Wait()
		// закрываем результирующий канал
		close(chOut)
	}()

	var count int64 // количество чисел результирующего канала
	var sum int64   // сумма чисел результирующего канала

	// 5. Читаем числа из результирующего канала
	// ...
	const MuxNumber = 100
	processedCount := 0

	for num := range chOut {
		sum += num
		count++
		processedCount++
		if processedCount == MuxNumber {
			parentCancel()
			break
		}
	}

	fmt.Println("Количество чисел", inputCount, count)
	fmt.Println("Сумма чисел", inputSum, sum)
	fmt.Println("Разбивка по каналам", amounts)

	// проверка результатов
	if inputSum != sum {
		log.Fatalf("Ошибка: суммы чисел не равны: %d != %d\n", inputSum, sum)
	}
	if inputCount != count {
		log.Fatalf("Ошибка: количество чисел не равно: %d != %d\n", inputCount, count)
	}
	for _, v := range amounts {
		inputCount -= v
	}
	if inputCount != 0 {
		log.Fatalf("Ошибка: разделение чисел по каналам неверное\n")
	}
}

package main

import (
	"context"
	"fmt"
	redis2 "github.com/go-redis/redis/v8"
	"github.com/joomcode/redispipe/redis"
	"github.com/joomcode/redispipe/rediscluster"
	protocol "github.com/quorzz/redis-protocol"
	"github.com/tidwall/resp"
	"go.uber.org/atomic"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	bulk        = 1000
	origin      = os.Getenv(`ORIGIN`)
	destination = os.Getenv(`DESTINATION`)
	originOps   = atomic.NewFloat64(0)
	originBytes = atomic.NewFloat64(0)
	destOps     = atomic.NewFloat64(0)
	destBytes   = atomic.NewFloat64(0)
)

func main() {
	fmt.Println(origin, destination)
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	go func() {
		t := time.NewTicker(time.Second)
		for range t.C {
			fmt.Print(fmt.Sprintf("fetching at %0.2fK ops @ %.02fMB/s, dumping at %0.2fK ops @ %.02fMB/s\n", originOps.Swap(0)/1000, originBytes.Swap(0)/1024/1024, destOps.Swap(0)/1000, destBytes.Swap(0)/1024/1024))
		}
	}()
	keys := changedKeys()
	withDump := getDump(keys)
	restore(withDump)
}

type Key struct {
	Key  string
	Dump []byte
}

func changedKeys() chan []Key {
	output := make(chan []Key, runtime.GOMAXPROCS(0)*64)
	go func() {
		defer close(output)
		r := redis2.NewClusterClient(&redis2.ClusterOptions{
			Addrs: strings.Split(origin, `,`),
		})
		slots, err := r.ClusterSlots(context.Background()).Result()
		if err != nil {
			panic(err)
		}
		wg := sync.WaitGroup{}

		for _, slot := range slots {
			wg.Add(1)
			slot := slot
			go func() {
				defer wg.Done()
				listen(slot.Nodes[0].Addr, output)
			}()
			wg.Add(1)
			go func() {
				defer wg.Done()
				scan(slot.Nodes[0].Addr, output)
			}()
		}
		wg.Wait()
	}()
	return output
}

func scan(addr string, output chan []Key) {
	r := redis2.NewClusterClient(&redis2.ClusterOptions{
		Addrs: strings.Split(addr, `,`),
	})
	buff := make([]Key, 0, bulk)
	offset := uint64(0)
	for {
		res, cursor, err := r.Scan(context.Background(), offset, `*`, 1000).Result()
		if err != nil {
			panic(err)
		}
		offset = cursor
		originOps.Add(float64(len(res)))
		for _, k := range res {
			buff = append(buff, Key{
				Key: k,
			})
			if len(buff) == cap(buff) {
				output <- buff
				buff = make([]Key, 0, bulk)
			}
		}
		if offset == 0 {
			fmt.Println("Done scanning")
			return
		}
	}
}

func listen(addr string, output chan []Key) {
	conn, _ := net.DialTimeout("tcp", addr, 10*time.Second)
	raw, _ := protocol.PackCommand("sync")
	conn.Write(raw)
	r := resp.NewReader(conn)
	buff := make([]Key, 0, bulk)
	for {
		v, _, _, err := r.ReadMultiBulk()
		if err != nil {
			fmt.Println(err)
			if err.Error() != `Protocol error: invalid bulk line ending` {
				break
			}
		}
		if arr := v.Array(); len(arr) > 1 {
			originOps.Add(1)
			buff = append(buff, Key{
				Key: arr[1].String(),
			})
			if len(buff) == cap(buff) {
				output <- buff
				buff = make([]Key, 0, bulk)
			}
		}
	}
	if len(buff) > 0 {
		output <- buff
	}
}

func getDump(keys chan []Key) chan []Key {
	output := make(chan []Key, runtime.GOMAXPROCS(0)*64)
	go func() {
		defer func() {
			close(output)
		}()
		ClusterRedis, err := func(ctx context.Context) (redis.Sender, error) {
			opts := rediscluster.Opts{
				Logger:       rediscluster.NoopLogger{}, // shut up logging. Could be your custom implementation.
				Name:         `ORIGIN`,
				ConnsPerHost: 64,
			}
			conn, err := rediscluster.NewCluster(ctx, strings.Split(origin, `,`), opts)
			return conn, err
		}(context.Background())
		if err != nil {
			panic(err)
		}
		r := redis.SyncCtx{S: ClusterRedis}
		wg := &sync.WaitGroup{}
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				reqs := make([]redis.Request, 0, bulk)
				for ks := range keys {
					for _, k := range ks {
						reqs = append(reqs, redis.Req(`DUMP`, k.Key))
					}
					resps := r.SendMany(context.Background(), reqs)
					for idx, rr := range resps {
						b, ok := rr.([]uint8)
						if ok {
							ks[idx].Dump = b
							originBytes.Add(float64(len(b)))
						}
					}
					reqs = reqs[:0]
					output <- ks
				}
			}()
		}
		wg.Wait()
	}()
	return output
}

func restore(keys chan []Key) {
	ClusterRedis, err := func(ctx context.Context) (redis.Sender, error) {
		opts := rediscluster.Opts{
			Logger:       rediscluster.NoopLogger{}, // shut up logging. Could be your custom implementation.
			ConnsPerHost: 64,
			Name:         `DESTINATION`,
		}
		conn, err := rediscluster.NewCluster(ctx, strings.Split(destination, `,`), opts)
		return conn, err
	}(context.Background())
	if err != nil {
		panic(err)
	}
	r := redis.SyncCtx{S: ClusterRedis}
	wg := &sync.WaitGroup{}
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reqs := make([]redis.Request, 0, bulk)
			for ks := range keys {
				for _, k := range ks {
					if len(k.Dump) > 0 {
						destBytes.Add(float64(len(k.Dump)))
						reqs = append(reqs, redis.Req(`RESTORE`, k.Key, 1000000, k.Dump, "REPLACE"))
					}
				}
				destOps.Add(float64(len(reqs)))
				resps := r.SendMany(context.Background(), reqs)
				for _, rr := range resps {
					if err := redis.AsError(rr); err != nil {
						panic(err)
					}
				}
				reqs = reqs[:0]
			}
		}()
	}
	wg.Wait()

}

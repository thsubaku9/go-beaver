package main

import (
	"beaver/btreeplus"
	"beaver/kvstore"
)

func main() {
	// btreeplus.Run()

	// fl, err := os.OpenFile("/Users/kernel/Documents/Playgrounds/go-beaver/go-beaver/temp.etc", os.O_RDWR|os.O_CREATE, 06444)
	// if err != nil {
	// 	println(err)
	// 	os.Exit(1)
	// }

	// fl.Truncate(1024)

	// fmt.Println(fl.Name())

	// mmap, err := unix.Mmap(int(fl.Fd()), 0, 100, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)

	// if err != nil {
	// 	println(err)
	// 	os.Exit(1)
	// }

	// copy(mmap, "hellorooni")
	// unix.Msync(mmap, unix.MS_SYNC)

	// unix.Munmap(mmap)
	// fl.Close()

	myStore := kvstore.ProvisionKV("/Users/kernel/Documents/Playgrounds/go-beaver/go-beaver/kvstore.data")

	err := myStore.Open()
	if err != nil {
		println(err)
	}

	// myStore.Set(btreeplus.ByteArr("k1"), btreeplus.ByteArr("mickey1"))
	// myStore.Set(btreeplus.ByteArr("k2"), btreeplus.ByteArr("mickey2"))
	// myStore.Set(btreeplus.ByteArr("k3"), btreeplus.ByteArr("the clan"))

	res, exists := myStore.Get(btreeplus.ByteArr("k1"))
	if exists {
		println(string(res))
	} else {
		println(exists)
	}

}

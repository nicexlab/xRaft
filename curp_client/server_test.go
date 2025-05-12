package main

// func TestConnect(t *testing.T) {
// 	go bench_server()
// 	MockClient()
// }

// func MockClient() {
// 	// 连接到服务器
// 	// 创建一个 Command
// 	cmd := Command{
// 		Op:    GET,
// 		Key:   "myKey",
// 		Value: "myValue",
// 	}

// 	// 创建一个缓冲区并创建一个 gob.Encoder
// 	buf := &bytes.Buffer{}
// 	enc := gob.NewEncoder(buf)

// 	// 使用 Encoder 编码 Command
// 	err := enc.Encode(cmd)
// 	if err != nil {
// 		log.Fatal("encode error:", err)
// 	}

// 	// 创建一个新的缓冲区来存储数据长度和数据本身
// 	finalBuf := &bytes.Buffer{}

// 	// 写入数据长度
// 	err = binary.Write(finalBuf, binary.LittleEndian, uint32(buf.Len()))
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	// 写入数据本身
// 	finalBuf.Write(buf.Bytes())

// 	// 连接到服务器
// 	conn, err := net.Dial("tcp", "localhost:9360")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer conn.Close()

// 	// 发送数据长度和数据本身
// 	conn.Write(finalBuf.Bytes())
// }

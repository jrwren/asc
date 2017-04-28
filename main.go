package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/storage"
)

const (
	prefix = "a:"
)

var (
	list string
)

func main() {
	listhelp := "list blobs or containers if - is used as string parameter"
	flag.StringVar(&list, "list", "", listhelp)
	flag.StringVar(&list, "l", "", listhelp)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage %s [-l] [%scontainer/]src [%scontainer/]dst:\n", os.Args[0], prefix, prefix)
		flag.PrintDefaults()
	}
	flag.Parse()

	act := os.Getenv("AZURE_STORAGE_ACCOUNT")
	key := os.Getenv("AZURE_STORAGE_KEY")

	c, err := storage.NewBasicClient(act, key)
	if err != nil {
		fmt.Println(err)
		if err.Error() == "azure: account name required" {
			fmt.Println("did you export AZURE_STORAGE_ACCOUNT?")
		} else if err.Error() == "azure: account key required" {
			fmt.Println("did you export AZURE_STORAGE_KEY?")
		}
		return
	}
	bsc := c.GetBlobService()
	if list == "-" {
		listContainers(bsc)
		return
	} else if list != "" {
		listBlobs(bsc, list)
		return
	}
	args := flag.Args()
	if len(args) < 2 {
		flag.Usage()
		return
	}
	dst := args[len(args)-1]
	sources := args[:len(args)-1]

	for _, src := range sources {
		fmt.Printf("copying %s to %s\n", src, dst)
		src_h, s, err := getReader(bsc, src)
		if err != nil {
			fmt.Println(err)
		}
		dst_h, err := getWriter(bsc, dst, s)
		if err != nil {
			fmt.Println(err)
		}
		_, err = io.Copy(dst_h, src_h)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func getReader(bsc storage.BlobStorageClient, f string) (r io.Reader, s int64, err error) {
	if strings.HasPrefix(f, prefix) {
		z := strings.SplitN(f[len(prefix):], "/", 2)
		var props *storage.BlobProperties
		r, props, err = bsc.GetBlobAndProperties(z[0], z[1])
		s = props.ContentLength
		return
	}
	h, err := os.Open(f)
	if err != nil {
		return
	}
	r = h
	st, err := h.Stat()
	if err != nil {
		return
	}
	s = st.Size()
	return
}

func getWriter(bsc storage.BlobStorageClient, f string, s int64) (io.Writer, error) {
	if strings.HasPrefix(f, prefix) {
		z := strings.SplitN(f[len(prefix):], "/", 2)
		reader, writer := io.Pipe()
		go func() {
			err := bsc.CreateBlockBlobFromReader(z[0], z[1], uint64(s), reader, nil)
			if err != nil {
				fmt.Println(err)
			}
		}()
		return writer, nil
	}
	return os.Create(f)
}

func listContainers(bsc storage.BlobStorageClient) {
	clr, err := bsc.ListContainers(storage.ListContainersParameters{})
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, bucket := range clr.Containers {
		fmt.Println(bucket.Name)
	}
}
func listBlobs(bsc storage.BlobStorageClient, container string) {
	c := bsc.GetContainerReference(container)
	blr, err := c.ListBlobs(storage.ListBlobsParameters{})
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, b := range blr.Blobs {
		fmt.Println(b.Name)
	}
}

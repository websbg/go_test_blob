package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/s3blob"
	"gopkg.in/ini.v1"
)

const (
	secretsFileName = "../../../d/odin/odin/odin_service_filesraw/secrets"
)

func main() {
	storeURL := ""
	useLocal := true
	ctx := context.Background()

	if useLocal {
		// Connect to a bucket when your program starts up.
		// This example uses the file-based implementation.
		dir, cleanup := newTempDir()
		defer cleanup()
		storeURL = "file://" + dir
	} else {
		cfg, err := ini.Load(secretsFileName)
		if err != nil {
			log.Fatal(err)
		}
		os.Setenv("AWS_ACCESS_KEY_ID", cfg.Section("").Key("AWS_ACCESS_KEY_ID").String())
		os.Setenv("AWS_SECRET_ACCESS_KEY", cfg.Section("").Key("AWS_SECRET_ACCESS_KEY").String())
		storeURL = "s3://odintest-filesraw?endpoint=fra1.digitaloceanspaces.com&region=us-east-1"
	}

	// Create the bucket.
	bucket, err := blob.OpenBucket(context.Background(), storeURL)
	if err != nil {
		log.Fatal(err)
	}
	defer bucket.Close()

	// clean the bucket
	var clean func(context.Context, *blob.Bucket, string)
	clean = func(ctx context.Context, b *blob.Bucket, prefix string) {
		iter := b.List(&blob.ListOptions{
			Delimiter: "/",
			Prefix:    prefix,
		})
		for {
			obj, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			if obj.IsDir {
				clean(ctx, b, obj.Key)
			} else {
				fmt.Printf("rm %s\n", obj.Key)
				b.Delete(ctx, obj.Key)
			}
		}
	}
	clean(ctx, bucket, "")
	fmt.Printf("clean!\n\n")

	// Create some blob objects in a hierarchy.
	for _, key := range []string{
		"testFile1",
		"t/t/t",
		"t-/t.",
		//"t0/t.", // instead of "t-/t." works well
		"dir1/testFile1dir1",
		"dir2/testFile1dir2",
		"d",
	} {
		if err := bucket.WriteAll(ctx, key, []byte("Go Cloud Development Kit"), nil); err != nil {
			log.Fatal(err)
		}
	}

	// list lists files in b starting with prefix. It uses the delimiter "/",
	// and recurses into "directories", adding 2 spaces to indent each time.
	// It will list the blobs created above because fileblob is strongly
	// consistent, but is not guaranteed to work on all services.
	var list func(context.Context, *blob.Bucket, string, string)
	list = func(ctx context.Context, b *blob.Bucket, prefix, indent string) {
		iter := b.List(&blob.ListOptions{
			Delimiter: "/",
			Prefix:    prefix,
		})
		for {
			obj, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s%s\n", indent, obj.Key)
			if obj.IsDir {
				list(ctx, b, obj.Key, indent+"  ")
			}
		}
	}
	list(ctx, bucket, "", "")

	// list cycles with decreasing page size

	for pageSize := 10; pageSize != 0; pageSize-- {
		fmt.Printf("\n\nlist with Prefix: \"\" and pageSize:%d\n\n", pageSize)

		opts := &blob.ListOptions{
			Prefix:    "",
			Delimiter: "/",
		}

		obs, token, err := bucket.ListPage(context.Background(), blob.FirstPageToken, pageSize, opts)
		for {
			if err != nil {
				log.Fatal(err)
			}

			for _, o := range obs {
				fmt.Printf("%s\n", o.Key)
			}

			if token == nil {
				break
			}

			obs, token, err = bucket.ListPage(context.Background(), token, pageSize, opts)
		}
	}
}

func newTempDir() (string, func()) {
	dir, err := ioutil.TempDir("", "go-cloud-blob-example")
	if err != nil {
		panic(err)
	}
	return dir, func() { os.RemoveAll(dir) }
}

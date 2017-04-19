package main

import (
	"encoding/json"
	"io"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"gopkg.in/cheggaaa/pb.v1"
	elastic "gopkg.in/olivere/elastic.v5"
	"io/ioutil"
)

type Hit struct {
	Source
	ID string
}

type Source struct {
	Code string `json:"code"`
}

const (
	indexName = "partner"
	typeName  = "dhisco"
	size      = 100 // https://www.elastic.co/guide/en/elasticsearch/guide/2.x/scroll.html
)

func main() {
	client, err := elastic.NewClient(
		elastic.SetSniff(false), // Set sniff to false to fix "no Elasticsearch node available" error
	)
	if err != nil {
		panic(err)
	}

	query := elastic.NewBoolQuery()
	query = query.Must(elastic.NewTermQuery("code", "no_matching"))

	// Count total and setup progress
	total, err := client.Count(indexName).Type(typeName).Query(query).Do(context.Background())
	if err != nil {
		panic(err)
	}
	bar := pb.StartNew(int(total))

	// This example illustrates how to use goroutines to iterate
	// through a result set via ScrollService.
	//
	// It uses the excellent golang.org/x/sync/errgroup package to do so.
	//
	// The first goroutine will Scroll through the result set and send
	// individual documents to a channel.
	//
	// The second cluster of goroutines will receive documents from the channel and
	// deserialize them.
	//
	// Feel free to add a third goroutine to do something with the
	// deserialized results.
	//
	// Let's go.

	// 1st goroutine sends individual hits to channel.
	// hits := make(chan json.RawMessage)
	hits := make(chan elastic.SearchHit)
	// The derived Context is canceled the first time a function passed to Go returns a non-nil error or
	// the first time Wait returns, whichever occurs first.
	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		defer close(hits)
		// Initialize scroller. Just don't call Do yet.
		scroll := client.Scroll(indexName).Type(typeName).Query(query).Size(size)
		for {
			results, err := scroll.Do(ctx)
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				// We save search hit instead of _source so that _id is included.
				hits <- *hit
			}

			// Check if we need to terminate early
			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	ch := make(chan Hit)
	// 2nd cluster of goroutines receive hits and deserializes them.
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			for hit := range hits {
				// Deserialize
				var p Source
				err := json.Unmarshal([]byte(*hit.Source), &p)
				if err != nil {
					return err
				}

				// Do something with the product here, e.g. send it to another channel
				// for further processing.
				ch <- Hit{ID: hit.Id, Source: p}

				bar.Increment()

				// Terminate early?
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}

	go func() {
		// Check whether any goroutines failed.
		if err := g.Wait(); err != nil {
			panic(err)
		}
		close(ch)
	}()

	// The main goroutine (not a 3rd goroutine) handles the deserialized results.
	// We do not print to stdout in order not to mess up the progress bar.
	var data []Hit
	for hit := range ch {
		data = append(data, hit)
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile("data.json", jsonData, 0644)
	if err != nil {
		panic(err)
	}
	
	// Done.
	bar.FinishPrint("Done")
}

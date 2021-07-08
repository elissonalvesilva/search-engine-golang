package shared

import (
	"bufio"
	"compress/gzip"
	"io"
)

func GZLines(rawf io.Reader, ch chan []byte, errs chan error, done chan bool) error {
	rawContents, err := gzip.NewReader(rawf)
	if err != nil {
		return err
	}

	bufferedContents := bufio.NewReader(rawContents)

	go func(ch chan []byte, errs chan error, contents *bufio.Reader, done chan bool) {
		for {
			line, err := contents.ReadBytes('\n')
			ch <- line
			if err != nil {
				if err == io.EOF {
					done <- true
				}
				if err != io.EOF {
					errs <- err
				}
				return
			}
		}
	}(ch, errs, bufferedContents, done)
	return nil
}

package httpd

import (
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

type rWCloser struct {
	w io.Writer
	r io.ReadCloser
}

func (rw rWCloser) Read(p []byte) (n int, err error) {
	return rw.r.Read(p)
}

func (rw rWCloser) Write(p []byte) (n int, err error) {
	return rw.w.Write(p)
}

func (rw rWCloser) Close() error {
	return rw.r.Close()
}
func (h *Handler) serveCreateIterator(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&h.stats.QueryRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&h.stats.QueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())
	w.WriteHeader(200)
	rw := rWCloser{
		w: w,
		r: r.Body,
	}

	h.ClusterService.CreateIterator(rw)
}
func (h *Handler) serveFieldDimensions(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	rw := rWCloser{
		w: w,
		r: r.Body,
	}
	h.ClusterService.FieldDimensions(rw)
}

package buffer

import "errors"

var BufferAbortException = errors.New("buffer request could not be satisfied")
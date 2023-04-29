package mvcc

import "errors"

var ErrNotFound = errors.New("not found")
var ErrAlreadyExists = errors.New("already exists")
var ErrRowInUse = errors.New("row in use")

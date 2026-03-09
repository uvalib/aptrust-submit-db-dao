//
//
//

package uvaaptsdao

import (
	"fmt"
	"time"
)

// error definitions
var ErrClientNotFound = fmt.Errorf("client not found")
var ErrSubmissionNotFound = fmt.Errorf("submission not found")
var ErrBagNotFound = fmt.Errorf("bag not found")
var ErrFileNotFound = fmt.Errorf("file not found")

type Client struct {
	Name       string    `json:"name"`       // client name
	Identifier string    `json:"identifier"` // client identifier
	Created    time.Time `json:"created"`    // created time
}

type Submission struct {
	Identifier string    `json:"identifier"` // submission identifier
	Client     string    `json:"client"`     // owning client
	Status     string    `json:"status"`     // current status
	Created    time.Time `json:"created"`    // created time
	Updated    time.Time `json:"updated"`    // updated time
}

type Bag struct {
	Name       string    `json:"name"`       // bag name
	Submission string    `json:"submission"` // owning submission
	Status     string    `json:"status"`     // current status
	ETag       string    `json:"etag"`       // current status
	Created    time.Time `json:"created"`    // created time
	Updated    time.Time `json:"updated"`    // updated time
}

type File struct {
	Name       string    `json:"name"`       // file name
	Submission string    `json:"submission"` // owning submission
	BagName    string    `json:"bag"`        // owning bag name
	Hash       string    `json:"hash"`       // file hash
	Created    time.Time `json:"created"`    // created time
}

type WhitelistedFile struct {
	Name    string    `json:"name"`    // file name
	Hash    string    `json:"hash"`    // file hash
	Created time.Time `json:"created"` // created time
}

//
// end of file
//

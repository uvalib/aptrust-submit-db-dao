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
	Id             int64     `json:"id"`              // record id
	Name           string    `json:"name"`            // client name
	Identifier     string    `json:"identifier"`      // client identifier
	DefaultStorage string    `json:"default_storage"` // default APTrust storage
	ApprovalEmail  string    `json:"approval_email"`  // if client requires manual approval
	Created        time.Time `json:"created"`         // created time
}

type Submission struct {
	Id             int64     `json:"id"`              // record id
	Identifier     string    `json:"identifier"`      // submission identifier
	CollectionName string    `json:"collection_name"` // collection name (if appropriate)
	Client         string    `json:"client"`          // owning client
	Storage        string    `json:"storage"`         // APTrust storage for this submission
	Created        time.Time `json:"created"`         // created time
}

type Bag struct {
	Id         int64     `json:"id"`         // record id
	Name       string    `json:"name"`       // bag name
	Submission string    `json:"submission"` // owning submission
	ETag       string    `json:"etag"`       // etag of submitted file
	Created    time.Time `json:"created"`    // created time
}

type File struct {
	Id         int64     `json:"id"`         // record id
	Name       string    `json:"name"`       // file name
	Submission string    `json:"submission"` // owning submission
	BagName    string    `json:"bag"`        // owning bag name
	Hash       string    `json:"hash"`       // file hash
	Created    time.Time `json:"created"`    // created time
}

type HashAllowEntry struct {
	Hash    string    `json:"hash"`    // file hash
	Comment string    `json:"comment"` // a helpful comment or explanation
	Created time.Time `json:"created"` // created time
}

type BagAllowEntry struct {
	Name    string    `json:"name"`    // bag name
	Comment string    `json:"comment"` // a helpful comment or explanation
	Created time.Time `json:"created"` // created time
}

type SubmissionState struct {
	Submission string    `json:"submission"` // owning submission
	State      string    `json:"state"`      // current state
	Updated    time.Time `json:"updated"`    // updated time
}

type BagState struct {
	Submission string    `json:"submission"` // owning submission
	Name       string    `json:"name"`       // bag name
	State      string    `json:"state"`      // current state
	Updated    time.Time `json:"updated"`    // updated time
}

//
// end of file
//

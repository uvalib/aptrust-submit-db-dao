//
//
//

package uvaaptsdao

import (
	"database/sql"
	"fmt"

	//"log"

	// postgres
	_ "github.com/lib/pq"
)

type Dao struct {
	//log     *log.Logger // logger
	*sql.DB // database connection
}

var initialSubmissionState = "registered"
var initialBagState = "registered"

func NewDao(host string, port int, user string, password string, dbname string) (*Dao, error) {

	// connection attributes
	connectionStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		host, port, user, password, dbname)

	// connect and ensure success
	db, err := sql.Open("postgres", connectionStr)
	if err != nil {
		fmt.Printf("ERROR: unable to open database (%s)\n", err.Error())
		return nil, err
	}

	// try a ping before declaring victory
	if err = db.Ping(); err != nil {
		fmt.Printf("ERROR: unable to ping database (%s)\n", err.Error())
		return nil, err
	}

	// all good
	return &Dao{
		//log:             c.Log,
		DB: db,
	}, nil
}

// Check -- check our database health
func (dao *Dao) Check() error {
	return dao.Ping()
}

//
// get methods
//

// GetSubmissionByIdentifier -- get the specified submission
func (dao *Dao) GetSubmissionByIdentifier(sid string) (*Submission, error) {

	rows, err := dao.Query("SELECT id, identifier, client, storage, collection_name, created_at FROM submissions WHERE identifier = $1 LIMIT 1", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	s, err := submissionQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// GetClientByIdentifier -- get the client details for the specified identifier
func (dao *Dao) GetClientByIdentifier(cid string) (*Client, error) {

	rows, err := dao.Query("SELECT id, name, identifier, default_storage, approval_email, created_at FROM clients WHERE identifier = $1 LIMIT 1", cid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	c, err := clientQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// GetBagBySubmissionAndName -- get the bag details for the specified submission and bag name
func (dao *Dao) GetBagBySubmissionAndName(sid string, name string) (*Bag, error) {

	rows, err := dao.Query("SELECT id, name, submission, etag, created_at FROM bags WHERE submission = $1 AND name = $2 LIMIT 1", sid, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	b, err := bagQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// GetBagsByStatus -- get a list of bags in the current state
func (dao *Dao) GetBagsByStatus(status string) ([]Bag, error) {

	rows, err := dao.Query("SELECT b.id, b.name, b.submission, b.etag, b.created_at FROM bags b, bag_state s1 WHERE s1.status = $1 AND s1.id = (SELECT max(id) FROM bag_state s2 WHERE s2.submission = b.submission AND s2.name = b.name)", status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bags, err := bagsQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return bags, nil
}

// GetBagsBySubmission -- get a list of bags in the specified submission
func (dao *Dao) GetBagsBySubmission(sid string) ([]Bag, error) {

	rows, err := dao.Query("SELECT id, name, submission, etag, created_at FROM bags WHERE submission = $1", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bags, err := bagsQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return bags, nil
}

// GetFilesBySubmission -- get a list of files in the specified submission
func (dao *Dao) GetFilesBySubmission(sid string) ([]File, error) {

	rows, err := dao.Query("SELECT id, name, hash, submission, bag_name, created_at FROM files WHERE submission = $1", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	files, err := filesQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return files, nil
}

// GetConflictFilesBySubmission -- get a list of conflicting files in the specified submission
func (dao *Dao) GetConflictFilesBySubmission(sid string) ([]File, error) {

	rows, err := dao.Query("SELECT f.id, f.name, f.hash, f.submission, f.bag_name, f.created_at FROM files f, apt_files a WHERE f.submission = $1 AND f.hash = a.hash", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	files, err := filesQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return files, nil
}

// GetAptFilesByHash -- get a list of APT files with the specified hash
func (dao *Dao) GetAptFilesByHash(hash string) ([]File, error) {

	rows, err := dao.Query("SELECT id, file_name, hash, '', bag_name, apt_added_at FROM apt_files WHERE hash = $1", hash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	files, err := filesQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return files, nil
}

// GetWhitelistedFiles -- get a list of files in the specified submission
func (dao *Dao) GetWhitelistedFiles() ([]WhitelistedFile, error) {

	rows, err := dao.Query("SELECT hash, comment, created_at FROM whitelist")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	files, err := whitelistFilesQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return files, nil
}

// GetSubmissionStateByIdentifier -- get the submission state of the specified identifier
func (dao *Dao) GetSubmissionStateByIdentifier(sid string) (*SubmissionState, error) {

	rows, err := dao.Query("SELECT submission, status, created_at FROM submission_state WHERE submission = $1 AND id = (SELECT MAX(id) FROM submission_state WHERE submission = $1)", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ss, err := submissionStateQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return ss, nil
}

// GetBagStateBySubmissionAndName -- get the bag state for the specified submission/bag name combination
func (dao *Dao) GetBagStateBySubmissionAndName(sid string, name string) (*BagState, error) {

	rows, err := dao.Query("SELECT name, submission, status, created_at FROM bag_state WHERE submission = $1 AND name = $2 AND id = (SELECT MAX(id) FROM bag_state WHERE submission = $1 AND name = $2)", sid, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bs, err := bagStateQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return bs, nil
}

// GetBagStateByName -- get the bag state for the specified bag name
// note this will get the status from the --latest-- submission as the same bag name
// can be submitted in multiple submissions
func (dao *Dao) GetBagStateByName(name string) (*BagState, error) {

	rows, err := dao.Query("SELECT name, submission, status, created_at FROM bag_state WHERE name = $1 AND id = (SELECT MAX(id) FROM bag_state WHERE name = $1)", name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bs, err := bagStateQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return bs, nil
}

//
// add methods
//

// AddSubmission -- add a new submission for the specified client
func (dao *Dao) AddSubmission(sid string, cid string, collection string, storage string) error {

	// insert into submissions
	stmt1, err := dao.Prepare("INSERT INTO submissions( identifier, client, collection_name, storage ) VALUES( $1,$2, $3, $4 )")
	if err != nil {
		return err
	}
	defer stmt1.Close()

	err = execPrepared(stmt1, sid, cid, collection, storage)
	if err != nil {
		return err
	}

	return dao.UpdateSubmissionState(sid, initialSubmissionState)
}

// AddBag -- add a new bag with the specified name and sibmission identifier
func (dao *Dao) AddBag(bagName string, sid string) error {

	// insert into bags
	stmt1, err := dao.Prepare("INSERT INTO bags( name, submission ) VALUES( $1,$2 )")
	if err != nil {
		return err
	}
	defer stmt1.Close()
	err = execPrepared(stmt1, bagName, sid)
	if err != nil {
		return err
	}
	return dao.UpdateBagState(bagName, sid, initialBagState)
}

// AddFile -- add a new file with the specified attributes
func (dao *Dao) AddFile(fileName string, hash string, sid string, bagName string) error {

	// insert into files
	stmt1, err := dao.Prepare("INSERT INTO files( name, hash, submission, bag_name ) VALUES( $1,$2, $3, $4 )")
	if err != nil {
		return err
	}
	defer stmt1.Close()
	return execPrepared(stmt1, fileName, hash, sid, bagName)
}

// AddApproval -- add a new approval with the specified attributes
func (dao *Dao) AddApproval(sid string, who string) error {

	// insert into files
	stmt1, err := dao.Prepare("INSERT INTO approvals( submission, who ) VALUES( $1,$2 )")
	if err != nil {
		return err
	}
	defer stmt1.Close()
	return execPrepared(stmt1, sid, who)
}

// AddConflict -- add a new conflict with the specified attributes
func (dao *Dao) AddConflict(sid string, newFileId int64, basis string, conflictFileId int64) error {

	// insert into files
	stmt1, err := dao.Prepare("INSERT INTO conflicts( submission, new_file, basis, conflicting_file ) VALUES( $1,$2, $3, $4 )")
	if err != nil {
		return err
	}
	defer stmt1.Close()
	return execPrepared(stmt1, sid, newFileId, basis, conflictFileId)
}

//
// update methods
//

func (dao *Dao) UpdateSubmissionState(sid string, state string) error {

	// insert into bag_state
	stmt1, err := dao.Prepare("INSERT INTO submission_state( submission, status ) VALUES( $1,$2 )")
	if err != nil {
		return err
	}
	defer stmt1.Close()
	return execPrepared(stmt1, sid, state)
}

func (dao *Dao) UpdateBagState(bagName string, sid string, state string) error {

	// insert into submission_state
	stmt1, err := dao.Prepare("INSERT INTO bag_state( name, submission, status ) VALUES( $1,$2,$3 )")
	if err != nil {
		return err
	}
	defer stmt1.Close()
	return execPrepared(stmt1, bagName, sid, state)
}

// UpdateBagETag - A special case where we update the bag etag after submitting to APT
func (dao *Dao) UpdateBagETag(bagName string, sid string, etag string) error {

	// update the bags table (this is the only case where we do this)
	stmt1, err := dao.Prepare("UPDATE bags SET etag = $1 WHERE name = $2 AND submission = $3")
	if err != nil {
		return err
	}
	defer stmt1.Close()
	return execPrepared(stmt1, etag, bagName, sid)
}

//
// internal helpers
//

func submissionQueryResults(rows *sql.Rows) (*Submission, error) {
	results := Submission{}
	count := 0

	for rows.Next() {
		err := rows.Scan(&results.Id, &results.Identifier, &results.Client, &results.Storage, &results.CollectionName, &results.Created)
		if err != nil {
			return nil, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrSubmissionNotFound)
	}

	//logDebug(log, fmt.Sprintf("found %d object(s)", count))
	return &results, nil
}

func clientQueryResults(rows *sql.Rows) (*Client, error) {
	results := Client{}
	count := 0

	for rows.Next() {
		err := rows.Scan(&results.Id, &results.Name, &results.Identifier, &results.DefaultStorage, &results.ApprovalEmail, &results.Created)
		if err != nil {
			return nil, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrClientNotFound)
	}

	//logDebug(log, fmt.Sprintf("found %d object(s)", count))
	return &results, nil
}

func bagQueryResults(rows *sql.Rows) (*Bag, error) {
	results := Bag{}
	count := 0

	for rows.Next() {
		err := rows.Scan(&results.Name, &results.Submission, &results.ETag, &results.Created)
		if err != nil {
			return nil, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrBagNotFound)
	}

	//logDebug(log, fmt.Sprintf("found %d object(s)", count))
	return &results, nil
}

func bagsQueryResults(rows *sql.Rows) ([]Bag, error) {
	results := make([]Bag, 0)
	count := 0

	for rows.Next() {
		bag := Bag{}
		err := rows.Scan(&bag.Id, &bag.Name, &bag.Submission, &bag.ETag, &bag.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, bag)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrBagNotFound)
	}

	//logDebug(log, fmt.Sprintf("found %d object(s)", count))
	return results, nil
}

func filesQueryResults(rows *sql.Rows) ([]File, error) {
	results := make([]File, 0)
	count := 0

	for rows.Next() {
		file := File{}
		err := rows.Scan(&file.Id, &file.Name, &file.Hash, &file.Submission, &file.BagName, &file.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, file)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrFileNotFound)
	}

	//logDebug(log, fmt.Sprintf("found %d object(s)", count))
	return results, nil
}

func whitelistFilesQueryResults(rows *sql.Rows) ([]WhitelistedFile, error) {
	results := make([]WhitelistedFile, 0)
	count := 0

	for rows.Next() {
		file := WhitelistedFile{}
		err := rows.Scan(&file.Hash, &file.Comment, &file.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, file)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrFileNotFound)
	}

	//logDebug(log, fmt.Sprintf("found %d object(s)", count))
	return results, nil
}

func submissionStateQueryResults(rows *sql.Rows) (*SubmissionState, error) {
	results := SubmissionState{}
	count := 0

	for rows.Next() {
		err := rows.Scan(&results.Submission, &results.State, &results.Updated)
		if err != nil {
			return nil, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrSubmissionNotFound)
	}

	//logDebug(log, fmt.Sprintf("found %d object(s)", count))
	return &results, nil
}

func bagStateQueryResults(rows *sql.Rows) (*BagState, error) {
	results := BagState{}
	count := 0

	for rows.Next() {
		err := rows.Scan(&results.Name, &results.Submission, &results.State, &results.Updated)
		if err != nil {
			return nil, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrBagNotFound)
	}

	//logDebug(log, fmt.Sprintf("found %d object(s)", count))
	return &results, nil
}

func execPrepared(stmt *sql.Stmt, values ...any) error {
	_, err := stmt.Exec(values...)
	return err
}

//
// end of file
//

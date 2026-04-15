//
//
//

package uvaaptsdao

import (
	//"log"

	// postgres
	_ "github.com/lib/pq"
)

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

	bags, err := bagListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return bags, nil
}

// GetBagsByName -- get a list of all bags of the specified name
func (dao *Dao) GetBagsByName(name string) ([]Bag, error) {

	rows, err := dao.Query("SELECT id, name, submission, etag, created_at FROM bags WHERE name = $1 ORDER BY id", name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bags, err := bagListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return bags, nil
}

// GetSubmissionsByStatus -- get a list of submissions in the current state
func (dao *Dao) GetSubmissionsByStatus(status string) ([]Submission, error) {

	rows, err := dao.Query("SELECT s.id, s.identifier, s.client, s.storage, s.collection_name, s.created_at FROM submissions s, submission_state s1 WHERE s1.status = $1 AND s1.id = (SELECT max(id) FROM submission_state s2 WHERE s2.submission = s.identifier)", status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	subs, err := submissionListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return subs, nil
}

// GetBagsBySubmission -- get a list of bags in the specified submission
func (dao *Dao) GetBagsBySubmission(sid string) ([]Bag, error) {

	rows, err := dao.Query("SELECT id, name, submission, etag, created_at FROM bags WHERE submission = $1", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bags, err := bagListQueryResults(rows)
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

	files, err := fileListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return files, nil
}

// GetAptHashConflictsBySubmission -- get a list of conflicting files in the specified submission
func (dao *Dao) GetAptHashConflictsBySubmission(sid string) ([]File, error) {

	rows, err := dao.Query("SELECT DISTINCT(f.id), f.name, f.hash, f.submission, f.bag_name, f.created_at FROM files f, apt_files a WHERE f.submission = $1 AND f.hash = a.hash", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	files, err := fileListQueryResults(rows)
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

	files, err := fileListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return files, nil
}

// GetHashAllowList -- get a list of hashes that should be 'ignored'
func (dao *Dao) GetHashAllowList() ([]HashAllowEntry, error) {

	rows, err := dao.Query("SELECT hash, comment, created_at FROM hash_allowlist")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	hashes, err := hashAllowListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return hashes, nil
}

// GetBagAllowList -- get a list of bags that should be 'ignored'
func (dao *Dao) GetBagAllowList() ([]BagAllowEntry, error) {

	rows, err := dao.Query("SELECT name, comment, created_at FROM bag_allowlist")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bags, err := bagAllowListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return bags, nil
}

// GetSubmissionStateByIdentifier -- get the submission state of the specified identifier
func (dao *Dao) GetSubmissionStateByIdentifier(sid string) (*SubmissionState, error) {

	rows, err := dao.Query("SELECT submission, status, created_at FROM submission_state WHERE submission = $1 AND id = (SELECT MAX(id) FROM submission_state WHERE submission = $1)", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	state, err := submissionStateQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return state, nil
}

// GetSubmissionStateHistoryByIdentifier -- get the submission state history for the specified identifier
func (dao *Dao) GetSubmissionStateHistoryByIdentifier(sid string) ([]SubmissionState, error) {

	rows, err := dao.Query("SELECT submission, status, created_at FROM submission_state WHERE submission = $1 ORDER BY id", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states, err := submissionStateListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return states, nil
}

// GetBagStateBySubmissionAndName -- get the bag state for the specified submission/bag name combination
func (dao *Dao) GetBagStateBySubmissionAndName(sid string, name string) (*BagState, error) {

	rows, err := dao.Query("SELECT name, submission, status, created_at FROM bag_state WHERE submission = $1 AND name = $2 AND id = (SELECT MAX(id) FROM bag_state WHERE submission = $1 AND name = $2)", sid, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	state, err := bagStateQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return state, nil
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

	state, err := bagStateQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return state, nil
}

func (dao *Dao) GetBagStateHistoryByName(name string) ([]BagState, error) {

	rows, err := dao.Query("SELECT name, submission, status, created_at FROM bag_state WHERE name = $1 ORDER BY submission, id", name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	states, err := bagStateListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return states, nil
}

// GetFailuresBySubmission -- get a list of (validation) failures in the specified submission
func (dao *Dao) GetFailuresBySubmission(sid string) ([]Failure, error) {

	rows, err := dao.Query("SELECT id, submission, failure, created_at FROM submission_failures WHERE submission = $1 ORDER BY id", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	failures, err := failureListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return failures, nil
}

// GetConflictsBySubmission -- get a list of conflicts in the specified submission
func (dao *Dao) GetConflictsBySubmission(sid string) ([]Conflict, error) {

	rows, err := dao.Query("SELECT c.submission, f.bag_name, f.name, a.bag_name, a.file_name, c.created_at FROM submission_conflicts c, files f, apt_files a WHERE c.submission = $1 AND f.id = c.new_file AND a.id = c.conflicting_file ORDER BY c.id", sid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	conflicts, err := conflictListQueryResults(rows)
	if err != nil {
		return nil, err
	}

	return conflicts, nil
}

//
// end of file
//

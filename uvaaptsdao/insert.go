//
//
//

package uvaaptsdao

import (
	//"log"

	// postgres
	_ "github.com/lib/pq"
)

var initialSubmissionState = "registered"
var initialBagState = "registered"

//
// add methods
//

// AddSubmission -- add a new submission for the specified client
func (dao *Dao) AddSubmission(sid string, cid string, collection string, storage string) error {

	// insert into submissions
	stmt, err := dao.Prepare("INSERT INTO submissions( identifier, client, collection_name, storage ) VALUES( $1,$2, $3, $4 )")
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = execPrepared(stmt, sid, cid, collection, storage)
	if err != nil {
		return err
	}

	return dao.UpdateSubmissionState(sid, initialSubmissionState)
}

// AddBag -- add a new bag with the specified name and sibmission identifier
func (dao *Dao) AddBag(bagName string, sid string) error {

	// insert into bags
	stmt, err := dao.Prepare("INSERT INTO bags( name, submission ) VALUES( $1,$2 )")
	if err != nil {
		return err
	}
	defer stmt.Close()
	err = execPrepared(stmt, bagName, sid)
	if err != nil {
		return err
	}
	return dao.UpdateBagState(bagName, sid, initialBagState)
}

// AddFile -- add a new file with the specified attributes
func (dao *Dao) AddFile(fileName string, hash string, sid string, bagName string) error {

	// insert into files
	stmt, err := dao.Prepare("INSERT INTO files( name, hash, submission, bag_name ) VALUES( $1,$2, $3, $4 )")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, fileName, hash, sid, bagName)
}

// AddApproval -- add a new approval with the specified attributes
func (dao *Dao) AddApproval(sid string, who string) error {

	// insert into files
	stmt, err := dao.Prepare("INSERT INTO approvals( submission, who ) VALUES( $1,$2 )")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid, who)
}

// AddConflict -- add a new conflict with the specified attributes
func (dao *Dao) AddConflict(sid string, newFileId int64, basis string, conflictFileId int64) error {

	// insert into files
	stmt, err := dao.Prepare("INSERT INTO submission_conflicts( submission, new_file, basis, conflicting_file ) VALUES( $1,$2, $3, $4 )")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid, newFileId, basis, conflictFileId)
}

// AddFailure -- add a new failure with the specified attributes
func (dao *Dao) AddFailure(sid string, reason string) error {

	// insert into files
	stmt, err := dao.Prepare("INSERT INTO submission_failures( submission, failure ) VALUES( $1,$2 )")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid, reason)
}

//
// end of file
//

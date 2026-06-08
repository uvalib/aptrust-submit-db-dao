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
// update methods
//

func (dao *Dao) UpdateSubmissionState(sid string, state string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.UpdateSubmissionState")
	defer funcExit()

	// insert into submission_states
	stmt, err := dao.Prepare("INSERT INTO submission_states( submission, status ) VALUES( $1,$2 )")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid, state)
}

func (dao *Dao) UpdateSubmissionStorage(sid string, storage string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.UpdateSubmissionStorage")
	defer funcExit()
	
	// update the submissions table (this is the only case where we do this)
	stmt, err := dao.Prepare("UPDATE submissions SET storage = $1 WHERE identifier = $2")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, storage, sid)
}

func (dao *Dao) UpdateBagState(bagName string, sid string, state string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.UpdateBagState")
	defer funcExit()

	// insert into bag_states
	stmt, err := dao.Prepare("INSERT INTO bag_states( bag_name, submission, status ) VALUES( $1,$2,$3 )")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, bagName, sid, state)
}

// UpdateBagETag - A special case where we update the bag etag after submitting to APT
func (dao *Dao) UpdateBagETag(bagName string, sid string, etag string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.UpdateBagETag")
	defer funcExit()

	// update the bags table (this is the only case where we do this)
	stmt, err := dao.Prepare("UPDATE bags SET etag = $1 WHERE bag_name = $2 AND submission = $3")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, etag, bagName, sid)
}

//
// end of file
//

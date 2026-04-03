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
// end of file
//

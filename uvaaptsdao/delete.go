//
//
//

package uvaaptsdao

//
// delete methods
//

func (dao *Dao) DeleteBagsBySubmission(sid string) error {

	stmt, err := dao.Prepare("DELETE FROM bags WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

func (dao *Dao) DeleteFilesBySubmission(sid string) error {

	stmt, err := dao.Prepare("DELETE FROM files WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

func (dao *Dao) DeleteFailuresBySubmission(sid string) error {

	stmt, err := dao.Prepare("DELETE FROM submission_failures WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

func (dao *Dao) DeleteConflictsBySubmission(sid string) error {

	stmt, err := dao.Prepare("DELETE FROM submission_conflicts WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

//
// end of file
//

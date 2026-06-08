//
//
//

package uvaaptsdao

//
// delete methods
//

func (dao *Dao) DeleteBagsBySubmission(sid string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.DeleteBagsBySubmission")
	defer funcExit()

	stmt, err := dao.Prepare("DELETE FROM bags WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

func (dao *Dao) DeleteFilesBySubmission(sid string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.DeleteFilesBySubmission")
	defer funcExit()

	stmt, err := dao.Prepare("DELETE FROM files WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

func (dao *Dao) DeleteFailuresBySubmission(sid string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.DeleteFailuresBySubmission")
	defer funcExit()

	stmt, err := dao.Prepare("DELETE FROM submission_failures WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

func (dao *Dao) DeleteConflictsBySubmission(sid string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.DeleteConflictsBySubmission")
	defer funcExit()

	stmt, err := dao.Prepare("DELETE FROM submission_conflicts WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

func (dao *Dao) DeleteApprovalsBySubmission(sid string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.DeleteApprovalsBySubmission")
	defer funcExit()

	stmt, err := dao.Prepare("DELETE FROM approvals WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

func (dao *Dao) DeleteBagStatesBySubmission(sid string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.DeleteBagStatesBySubmission")
	defer funcExit()

	stmt, err := dao.Prepare("DELETE FROM bag_states WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

func (dao *Dao) DeleteSubmissionStatesBySubmission(sid string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.DeleteSubmissionStatesBySubmission")
	defer funcExit()

	stmt, err := dao.Prepare("DELETE FROM submission_states WHERE submission = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

func (dao *Dao) DeleteSubmission(sid string) error {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.DeleteSubmission")
	defer funcExit()

	stmt, err := dao.Prepare("DELETE FROM submissions WHERE identifier = $1")
	if err != nil {
		return err
	}
	defer stmt.Close()
	return execPrepared(stmt, sid)
}

//
// end of file
//

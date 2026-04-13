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

func submissionsQueryResults(rows *sql.Rows) ([]Submission, error) {
	results := make([]Submission, 0)
	count := 0

	for rows.Next() {
		sub := Submission{}
		err := rows.Scan(&sub.Id, &sub.Identifier, &sub.Client, &sub.Storage, &sub.CollectionName, &sub.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, sub)
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
	return results, nil
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

func hashAllowListQueryResults(rows *sql.Rows) ([]HashAllowEntry, error) {
	results := make([]HashAllowEntry, 0)
	count := 0

	for rows.Next() {
		file := HashAllowEntry{}
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

func bagAllowListQueryResults(rows *sql.Rows) ([]BagAllowEntry, error) {
	results := make([]BagAllowEntry, 0)
	count := 0

	for rows.Next() {
		file := BagAllowEntry{}
		err := rows.Scan(&file.Name, &file.Comment, &file.Created)
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
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrBagNotFound)
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

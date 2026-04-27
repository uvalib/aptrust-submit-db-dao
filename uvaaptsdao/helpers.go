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

	//fmt.Printf("found %d object(s)\n", count)
	return &results, nil
}

func submissionListQueryResults(rows *sql.Rows) ([]Submission, error) {
	results := make([]Submission, 0)
	count := 0

	for rows.Next() {
		result := Submission{}
		err := rows.Scan(&result.Id, &result.Identifier, &result.Client, &result.Storage, &result.CollectionName, &result.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrSubmissionNotFound)
	}

	//fmt.Printf("found %d object(s)\n", count)
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

	//fmt.Printf("found %d object(s)\n", count)
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

	//fmt.Printf("found %d object(s)\n", count)
	return &results, nil
}

func bagListQueryResults(rows *sql.Rows) ([]Bag, error) {
	results := make([]Bag, 0)
	count := 0

	for rows.Next() {
		result := Bag{}
		err := rows.Scan(&result.Id, &result.Name, &result.Submission, &result.ETag, &result.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrBagNotFound)
	}

	//fmt.Printf("found %d object(s)\n", count)
	return results, nil
}

func fileListQueryResults(rows *sql.Rows) ([]File, error) {
	results := make([]File, 0)
	count := 0

	for rows.Next() {
		result := File{}
		err := rows.Scan(&result.Id, &result.Name, &result.BagName, &result.Submission, &result.Hash, &result.Size, &result.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrFileNotFound)
	}

	//fmt.Printf("found %d object(s)\n", count)
	return results, nil
}

func hashAllowListQueryResults(rows *sql.Rows) ([]HashAllowEntry, error) {
	results := make([]HashAllowEntry, 0)
	count := 0

	for rows.Next() {
		result := HashAllowEntry{}
		err := rows.Scan(&result.Hash, &result.Comment, &result.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrFileNotFound)
	}

	//fmt.Printf("found %d object(s)\n", count)
	return results, nil
}

func bagAllowListQueryResults(rows *sql.Rows) ([]BagAllowEntry, error) {
	results := make([]BagAllowEntry, 0)
	count := 0

	for rows.Next() {
		result := BagAllowEntry{}
		err := rows.Scan(&result.Name, &result.Comment, &result.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrBagNotFound)
	}

	//fmt.Printf("found %d object(s)\n", count)
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

	//fmt.Printf("found %d object(s)\n", count)
	return &results, nil
}

func submissionStateListQueryResults(rows *sql.Rows) ([]SubmissionState, error) {
	results := make([]SubmissionState, 0)
	count := 0

	for rows.Next() {
		result := SubmissionState{}
		err := rows.Scan(&result.Submission, &result.State, &result.Updated)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrSubmissionNotFound)
	}

	//fmt.Printf("found %d object(s)\n", count)
	return results, nil
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

	//fmt.Printf("found %d object(s)\n", count)
	return &results, nil
}

func bagStateListQueryResults(rows *sql.Rows) ([]BagState, error) {
	results := make([]BagState, 0)
	count := 0

	for rows.Next() {
		result := BagState{}
		err := rows.Scan(&result.Name, &result.Submission, &result.State, &result.Updated)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrBagNotFound)
	}

	//fmt.Printf("found %d object(s)\n", count)
	return results, nil
}

func failureListQueryResults(rows *sql.Rows) ([]Failure, error) {
	results := make([]Failure, 0)
	count := 0

	for rows.Next() {
		result := Failure{}
		err := rows.Scan(&result.Id, &result.Submission, &result.Failure, &result.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrFailureNotFound)
	}

	//fmt.Printf("found %d object(s)\n", count)
	return results, nil
}

func conflictListQueryResults(rows *sql.Rows) ([]Conflict, error) {
	results := make([]Conflict, 0)
	count := 0

	for rows.Next() {
		result := Conflict{}
		err := rows.Scan(&result.Submission, &result.BagName, &result.FileName, &result.Hash, &result.ConflictBagName, &result.ConflictFileName, &result.Created)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		count++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// check for not found
	if count == 0 {
		return nil, fmt.Errorf("%q: %w", "object(s) not found", ErrConflictNotFound)
	}

	//fmt.Printf("found %d object(s)\n", count)
	return results, nil
}

func execPrepared(stmt *sql.Stmt, values ...any) error {
	_, err := stmt.Exec(values...)
	return err
}

//
// end of file
//

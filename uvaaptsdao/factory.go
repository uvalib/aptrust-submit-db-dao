//
//
//

package uvaaptsdao

import (
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strconv"

	//"log"

	// postgres
	_ "github.com/lib/pq"
)

type Dao struct {
	//log     *log.Logger // logger
	*sql.DB // database connection
}

func NewDao(host string, port int, user string, password string, dbname string) (*Dao, error) {

	// log function entry and exit
	funcExit := funcEntry("uvaaptsdao.NewDao")
	defer funcExit()

	// connection attributes, built as a URL so credentials containing
	// spaces, quotes or other special characters are escaped correctly
	connectionUrl := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, password),
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		Path:   "/" + dbname,
	}
	connectionStr := connectionUrl.String()

	// connect and ensure success
	db, err := sql.Open("postgres", connectionStr)
	if err != nil {
		fmt.Printf("ERROR: unable to open database (%s)\n", err.Error())
		return nil, err
	}

	// try a ping before declaring victory
	if err = db.Ping(); err != nil {
		fmt.Printf("ERROR: unable to ping database (%s)\n", err.Error())
		// release the connection pool, we are not returning it
		db.Close()
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
// end of file
//

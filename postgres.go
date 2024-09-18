package sqlhelp

import (
	"net/url"
	"strings"
)

// AddSearchPath adds a search path to postgres dsn string.
func AddSearchPath(dsn string, schema ...string) string {
	searchPath := "search_path=" + strings.Join(schema, ",")
	uri, err := url.Parse(dsn)
	if err != nil || !strings.Contains(dsn, "://") {
		// this accounts for "host=... user=... etc=..." conn string
		return dsn + " " + searchPath
	}

	vals := uri.Query()
	vals.Add("options", "--"+searchPath)
	uri.RawQuery = vals.Encode()
	return uri.String()
}

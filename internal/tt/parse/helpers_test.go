package parse_test

import (
	"strconv"

	"github.com/tidwall/gjson"
)

func jsonResult(s string) gjson.Result { return gjson.Parse(s) }

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

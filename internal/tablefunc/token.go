package tablefunc

import (
	"fmt"
	"net/url"
	"strings"
)

const Prefix = "__fssql_table_args_v1__?"

// Encode freezes static table-function options into a deterministic internal
// token. Values are URL-escaped and sorted by url.Values.Encode.
func Encode(source string, options map[string]string) string {
	values:=url.Values{};values.Set("source",source);for key,value:=range options{values.Set(strings.ToLower(strings.TrimSpace(key)),value)};return Prefix+values.Encode()
}

// Decode returns encoded=false for legacy one-source table function strings.
func Decode(token string)(source string,options map[string]string,encoded bool,err error){if !strings.HasPrefix(token,Prefix){return token,nil,false,nil};values,parseErr:=url.ParseQuery(strings.TrimPrefix(token,Prefix));if parseErr!=nil{return "",nil,true,parseErr};source=values.Get("source");if source==""{return "",nil,true,fmt.Errorf("table function source is empty")};options=make(map[string]string);for key,vals:=range values{if key=="source"{continue};if len(vals)!=1{return "",nil,true,fmt.Errorf("table function option %s is repeated",key)};options[key]=vals[0]};return source,options,true,nil}

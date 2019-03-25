/*
   Copyright 2019 Simon Schmidt

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


package myschema

import "strings"
import "gopkg.in/src-d/go-vitess.v1/vt/sqlparser"
import "gopkg.in/src-d/go-vitess.v1/vt/vtgate/vindexes"
//import "fmt"
import "io"

func indexUsing(options []*sqlparser.IndexOption) string {
	for _,o := range options {
		if o.Using!="" { return o.Using }
	}
	return ""
}

type ks_builder struct{
	*vindexes.KeyspaceSchema
}
func (ks ks_builder) add(ddl *sqlparser.DDL) error {
	println(ddl.Action)
	switch ddl.Action {
	case "create":
		if ddl.TableSpec!=nil {
			return ks.addTable(ddl)
		}
	}
	return nil
}
func (ks ks_builder) addVindex(tableName string,idx *sqlparser.IndexInfo,options []*sqlparser.IndexOption) (index vindexes.Vindex,owned,ordered bool,err error) {
	var ok bool
	using := indexUsing(options)
	if !strings.HasPrefix(using,"v_") { return nil,false,false,nil }
	name := idx.Name.String()
	
	if idx.Primary {
		name = tableName+"_primary_key"
		owned = true
	} else if idx.Spatial {
		return nil,false,false,nil
	}
	
	index,ok = ks.Vindexes[name]
	if ok { return }
	
	switch using {
	case "v_binary": index,err = vindexes.NewBinary(name,nil)
	case "v_md5": index,err = vindexes.NewBinaryMD5(name,nil)
	case "v_hash": index,err = vindexes.NewHash(name,nil)
	}
	
	if index!=nil { ks.Vindexes[name] = index }
	
	return
}
func (ks ks_builder) addTable(ddl *sqlparser.DDL) error {
	n := ddl.Table.Name.String()
	
	if strings.Contains(ddl.TableSpec.Options,"type=vindex") {
		from := ""
		to := ""
		for _,col := range ddl.TableSpec.Columns {
			na := col.Name.String()
			if strings.HasPrefix(na,"f") {
				from += ","+na
			}
			if strings.HasPrefix(na,"t") && to=="" {
				to = na
			}
		}
		vdx,err := vindexes.NewLookup(n,map[string]string{"table":n,"from":from,"to":to,"autocommit":"true"})
		if err!=nil { return err }
		ks.Vindexes[n] = vdx
		return nil
	}
	
	tab := new(vindexes.Table)
	tab.Name = ddl.Table.Name
	tab.Keyspace = ks.Keyspace
	ks.Tables[n] = tab
	
	tab.Columns = make([]vindexes.Column,len(ddl.TableSpec.Columns))
	for i,col := range ddl.TableSpec.Columns {
		tab.Columns[i].Name = col.Name
		tab.Columns[i].Type = col.Type.SQLType()
	}
	
	for _,index := range ddl.TableSpec.Indexes {
		vdx,owned,ordered,err := ks.addVindex(n,index.Info,index.Options)
		if err!=nil { return err }
		if vdx==nil { continue }
		cvi := new(vindexes.ColumnVindex)
		cvi.Type = "index"
		cvi.Name = vdx.String()
		cvi.Owned = owned
		cvi.Vindex = vdx
		tab.ColumnVindexes = append(tab.ColumnVindexes,cvi)
		if owned {
			tab.Owned = append(tab.Owned,cvi)
		}
		if ordered {
			tab.Ordered = append(tab.Ordered,cvi)
		}
	}
	return nil
}

func LoadScript(tok *sqlparser.Tokenizer,defschem string,sharded bool) (*vindexes.VSchema,error) {
	ks := make(map[string]ks_builder)
	vs := new(vindexes.VSchema)
	vs.Keyspaces = make(map[string]*vindexes.KeyspaceSchema)
	
	createKs := func(n string){
		k := &vindexes.Keyspace{Name:n,Sharded:sharded}
		s := &vindexes.KeyspaceSchema{
			Keyspace: k,
			Tables: make(map[string]*vindexes.Table),
			Vindexes: make(map[string]vindexes.Vindex),
		}
		ks[n] = ks_builder{s}
		vs.Keyspaces[n] = s
	}
	
	for {
		stmt,err := sqlparser.ParseNext(tok)
		if err==io.EOF{ break }
		if err!=nil { return vs,err }
		switch v := stmt.(type){
		case *sqlparser.DBDDL: createKs(v.DBName)
		case *sqlparser.DDL:
			schem := v.Table.Qualifier.String()
			if schem=="" { schem = defschem }
			s,ok := ks[schem]
			if !ok { createKs(schem) }
			s,ok = ks[schem]
			if !ok { panic("unreachable") }
			s.add(v)
		}
	}
	return vs,nil
}


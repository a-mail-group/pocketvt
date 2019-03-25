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


package ctxschema

import (
	key "gopkg.in/src-d/go-vitess.v1/vt/key"
	
	topodatapb "gopkg.in/src-d/go-vitess.v1/vt/proto/topodata"
	
	"gopkg.in/src-d/go-vitess.v1/vt/sqlparser"
	
	"gopkg.in/src-d/go-vitess.v1/vt/vtgate/planbuilder"
	"gopkg.in/src-d/go-vitess.v1/vt/vtgate/vindexes"
)

type Err int
const (
	ERR_NO_DEFAULT_KEYSPACE Err = iota
	ERR_KEYSPACE_NOT_FOUND
)
func (e Err) Error() string {
	switch e {
	case ERR_NO_DEFAULT_KEYSPACE: return "ERR_NO_DEFAULT_KEYSPACE"
	case ERR_KEYSPACE_NOT_FOUND: return "ERR_KEYSPACE_NOT_FOUND"
	}
	return "???"
}

type Splitter interface{
	Split(qual,name string) (tt topodatapb.TabletType,dest key.Destination,nname string)
}
type DefaultSplitter struct{}
func (DefaultSplitter) Split(qual,name string) (tt topodatapb.TabletType,dest key.Destination,nname string) {
	tt = topodatapb.TabletType_MASTER
	nname = qual
	return
}
func orDefaultSpl(spl Splitter) Splitter {
	if spl==nil { spl = DefaultSplitter{} }
	return spl
}

type SpecialSplitter struct{}
func (SpecialSplitter) Split(qual,name string) (tt topodatapb.TabletType,dest key.Destination,nname string) {
	tt = topodatapb.TabletType_MASTER
	nname = qual
	dest = key.DestinationKeyspaceID(name)
	return
}

type ContextSchema struct{
	Splitter Splitter
	DefKS *vindexes.Keyspace
	VSchm *vindexes.VSchema
}
func MakeContextSchema(spl Splitter,defks string,schema *vindexes.VSchema) (*ContextSchema,error) {
	ctx := new(ContextSchema)
	ctx.Splitter = orDefaultSpl(spl)
	ctx.VSchm = schema
	
	if defks=="" { return ctx,nil  }
	
	dksp,ok := ctx.VSchm.Keyspaces[defks]
	if !ok { return nil,ERR_KEYSPACE_NOT_FOUND }
	
	ctx.DefKS = dksp.Keyspace
	
	return ctx,nil
}


func (s *ContextSchema) FindTable(tablename sqlparser.TableName) (tab *vindexes.Table, ksp string, tt topodatapb.TabletType, dest key.Destination, err error) {
	tt,dest,ksp = s.Splitter.Split(tablename.Qualifier.String(),tablename.Name.String())
	if ksp=="" {
		if s.DefKS==nil { err = ERR_NO_DEFAULT_KEYSPACE; return }
		ksp = s.DefKS.Name
	}
	
	tab,err = s.VSchm.FindTable(ksp,tablename.Name.String())
	
	return
}
func (s *ContextSchema) FindTableOrVindex(tablename sqlparser.TableName) (tab *vindexes.Table, ind vindexes.Vindex, ksp string, tt topodatapb.TabletType, dest key.Destination, err error) {
	tt,dest,ksp = s.Splitter.Split(tablename.Qualifier.String(),tablename.Name.String())
	if ksp=="" {
		if s.DefKS==nil { err = ERR_NO_DEFAULT_KEYSPACE; return }
		ksp = s.DefKS.Name
	}
	
	tab,ind,err = s.VSchm.FindTableOrVindex(ksp,tablename.Name.String())
	
	return
}

func (s *ContextSchema) DefaultKeyspace() (*vindexes.Keyspace, error) {
	if s.DefKS==nil { return nil,ERR_NO_DEFAULT_KEYSPACE }
	return s.DefKS,nil
}
func (s *ContextSchema) TargetString() string {
	return "target"
}

var (
	_ planbuilder.ContextVSchema
)


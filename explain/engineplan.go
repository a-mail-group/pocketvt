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


package explain

import "gopkg.in/src-d/go-vitess.v1/vt/vtgate/engine"
import "reflect"

func Primitive2String(p engine.Primitive) string {
	if p==nil { return "<nil>" }
	tree := NewTreePrinter()
	switch v := p.(type) {
	case *engine.Route:
		tree.WriteNlf("Route",
			v.Opcode,
			v.Keyspace,
			v.TargetDestination,
			v.TargetTabletType,
			v.Query,
			v.FieldQuery,
			v.Vindex,
			v.Values,
			v.OrderBy,
			v.TruncateColumnCount,
			v.QueryTimeout,
			v.ScatterErrorsAsWarnings,
		)
	case *engine.Insert:
		tree.WriteNlf("Insert",
			v.Opcode,
			v.Keyspace,
			v.Query,
			v.VindexValues,
			v.Generate,
			"'"+v.Prefix+"'",
			v.Mid,
			"'"+v.Suffix+"'",
			v.MultiShardAutocommit,
		)
	default:
		tree.WriteNode("%v%v",reflect.ValueOf(p).Type(),p)	
	}
	
	return tree.String()
}

package internal

import "reflect"

func InstanceTypeName(instance any) string {
	t := reflect.TypeOf(instance)
	return t.String()
}

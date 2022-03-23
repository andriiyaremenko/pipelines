package internal

import "reflect"

func ZeroValue[T any]() T {
	var nilValue T
	return nilValue
}

func TypeName[T any]() string {
	t := reflect.TypeOf((*T)(nil))
	return t.Elem().String()
}

func InstanceTypeName(instance any) string {
	t := reflect.TypeOf(instance)
	return t.String()
}

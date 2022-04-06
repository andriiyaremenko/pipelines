package internal

import "reflect"

func Zero[T any]() T {
	var zero T
	return zero
}

func InstanceTypeName(instance any) string {
	t := reflect.TypeOf(instance)
	return t.String()
}

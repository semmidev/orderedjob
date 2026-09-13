package handler

import orderedjob "github.com/semmidev/orderedjob"

type Handler = orderedjob.Handler
type HandlerFunc = orderedjob.HandlerFunc
type Registry = orderedjob.Registry

var NewRegistry = orderedjob.NewRegistry

package obrc

import (
	"io"
)

// Calculator is an interface for custom Calculate implementations.
type Calculator interface {
	Calculate(inputFile string, output io.Writer) error
}

// CalculateFunc is a function type that implements the Calculator interface.
type CalculateFunc func(inputFile string, output io.Writer) error

// Calculate calls the CalculateFunc with the provided input,
// allowing CalculateFunc to be used as a Calculator implementation.
func (f CalculateFunc) Calculate(inputFile string, output io.Writer) error {
	return f(inputFile, output)
}
